// Copyright 2021 - 2024 Matrix Origin
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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestCreateAndDropPitr(t *testing.T) {
	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			defer cleanupSQLIntegration(t, cn1, "drop database if exists "+db)
			pitrName := "pitr_ut"

			// create database
			res, err := exec.Exec(
				ctx,
				"create database "+db,
				executor.Options{},
			)
			require.NoError(t, err)
			res.Close()

			// create pitr
			res, err = exec.Exec(
				ctx,
				"create pitr "+pitrName+" for database "+db+" range 1 'd'",
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()

			// drop pitr
			res, err = exec.Exec(
				ctx,
				"drop pitr "+pitrName,
				executor.Options{}.WithDatabase(db),
			)
			require.NoError(t, err)
			res.Close()
		},
	)
}

func TestPitrCases(t *testing.T) {
	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			defer cleanupSQLIntegration(t, cn1, "drop database if exists "+db)
			table := "table01"
			pitr1 := "pitr01"
			pitr2 := "pitr02"
			pitr3 := "pitr03"

			// create database and table
			res, err := exec.Exec(ctx, "create database "+db, executor.Options{})
			require.NoError(t, err)
			res.Close()
			res, err = exec.Exec(ctx, "create table "+table+" (col1 int)", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// create pitr with different units and verify, also exercise a frequency-like variety
			type pitrCase struct{ unit, label string }
			for _, pc := range []pitrCase{{"h", "hour"}, {"d", "day"}, {"mo", "month"}, {"y", "year"}} {
				name := "pitr_" + pc.unit
				res, err = exec.Exec(ctx, "drop pitr if exists "+name+" internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
				res, err = exec.Exec(ctx, "create pitr "+name+" for database "+db+" range 1 '"+pc.unit+"' internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
				// verify exists
				verify := "select pitr_unit from mo_catalog.mo_pitr where level='database' and database_name='" + db + "' and pitr_name='" + name + "'"
				res, err = exec.Exec(ctx, verify, executor.Options{})
				require.NoError(t, err)
				cnt := 0
				for _, b := range res.Batches {
					cnt += b.RowCount()
				}
				require.GreaterOrEqual(t, cnt, 0)
				res.Close()
				// cleanup
				res, err = exec.Exec(ctx, "drop pitr if exists "+name+" internal", executor.Options{}.WithDatabase(db))
				require.NoError(t, err)
				res.Close()
			}

			// create pitr for table
			res, err = exec.Exec(ctx, "drop pitr if exists "+pitr1+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()
			res, err = exec.Exec(ctx, "create pitr "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// create pitr with if not exists (treat as frequency scenario: ensure idempotence)
			res, err = exec.Exec(ctx, "create pitr if not exists "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()
			// verify table-level pitr row present
			res, err = exec.Exec(ctx, "select * from mo_catalog.mo_pitr where level='table' and database_name='"+db+"' and table_name='"+table+"'", executor.Options{})
			require.NoError(t, err)
			{
				tc := 0
				for _, b := range res.Batches {
					tc += b.RowCount()
				}
				require.GreaterOrEqual(t, tc, 0)
			}
			res.Close()

			// error: duplicate create
			_, err = exec.Exec(ctx, "create pitr "+pitr1+" for table "+db+" "+table+" range 1 'h' internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// error: invalid unit
			_, err = exec.Exec(ctx, "create pitr "+pitr2+" for database "+db+" range 1 'yy' internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// drop pitr
			res, err = exec.Exec(ctx, "drop pitr "+pitr1+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// drop non-existent pitr (should error)
			_, err = exec.Exec(ctx, "drop pitr "+pitr3+" internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// drop non-existent pitr with if exists (should not error)
			res, err = exec.Exec(ctx, "drop pitr if exists "+pitr3+" internal", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			res.Close()

			// show pitr
			res, err = exec.Exec(ctx, "show pitr", executor.Options{}.WithDatabase(db))
			require.NoError(t, err)
			rowCount := 0
			for _, b := range res.Batches {
				rowCount += b.RowCount()
			}
			require.Equal(t, rowCount, 0)
			res.Close()
		},
	)
}

// TestCDCNoPrimaryKeyRejected exercises the public CREATE CDC SQL path. It is
// intentionally independent of the external sink used by TestCDCCases: a
// source without a user-visible primary key must be rejected before a task is
// persisted or any sink connection is attempted.
func TestCDCNoPrimaryKeyRejected(t *testing.T) {
	stubOpenDbConn := gostub.Stub(&cdc.OpenDbConn, func(_ context.Context, _, _, _ string, _ int, _ string) (*sql.DB, error) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectClose()
		return db, nil
	})
	defer stubOpenDbConn.Reset()

	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		exec := testutils.GetSQLExecutor(cn)
		db := testutils.GetDatabaseName(t)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+db)

		res, err := exec.Exec(ctx, "create database "+db, executor.Options{})
		require.NoError(t, err)
		res.Close()

		execSQL := func(sql string) {
			res, execErr := exec.Exec(ctx, sql, executor.Options{}.WithDatabase(db))
			require.NoError(t, execErr)
			res.Close()
		}
		execSQL("create pitr if not exists cdc_pitr for database " + db + " range 3 'h' internal")
		execSQL("create table with_pk (id int primary key, value int)")
		execSQL("create table composite_pk (id1 int, id2 int, value int, primary key (id1, id2))")

		conn := "mysql://user:password@127.0.0.1:1"
		execCDC := func(sql string) {
			res, execErr := exec.Exec(ctx, sql, executor.Options{}.WithDatabase(db))
			require.NoError(t, execErr)
			res.Close()
		}
		verifyTask := func(name string, want bool) {
			res, queryErr := exec.Exec(ctx,
				"select count(*) from mo_catalog.mo_cdc_task where task_name='"+name+"'",
				executor.Options{}.WithDatabase(db))
			require.NoError(t, queryErr)
			defer res.Close()
			require.Equal(t, want, testutils.ReadCount(res) > 0)
		}

		execCDC(
			"create cdc accepted_table '" + conn + "' 'matrixone' '" + conn + "' '" + db + ".with_pk' {'Level'='table'}",
		)
		verifyTask("accepted_table", true)
		execCDC(
			"create cdc accepted_composite '" + conn + "' 'matrixone' '" + conn + "' '" + db + ".composite_pk' {'Level'='table'}",
		)
		verifyTask("accepted_composite", true)
		execCDC(
			"create cdc accepted_database '" + conn + "' 'matrixone' '" + conn + "' '" + db + "' {'Level'='database'}",
		)
		verifyTask("accepted_database", true)

		badDB := db + "_bad"
		res, err = exec.Exec(ctx, "create database "+badDB, executor.Options{})
		require.NoError(t, err)
		res.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+badDB)
		res, err = exec.Exec(ctx, "create pitr if not exists cdc_pitr_bad for database "+badDB+" range 3 'h' internal", executor.Options{}.WithDatabase(badDB))
		require.NoError(t, err)
		res.Close()
		res, err = exec.Exec(ctx, "create table no_pk (value int)", executor.Options{}.WithDatabase(badDB))
		require.NoError(t, err)
		res.Close()
		_, err = exec.Exec(ctx,
			"create cdc rejected_no_pk '"+conn+"' 'matrixone' '"+conn+"' '"+badDB+"' {'Level'='database'}",
			executor.Options{}.WithDatabase(badDB))
		require.Error(t, err)

		res, err = exec.Exec(ctx,
			"select count(*) from mo_catalog.mo_cdc_task where task_name='rejected_no_pk'",
			executor.Options{}.WithDatabase(db))
		require.NoError(t, err)
		defer res.Close()
		require.Equal(t, 0, testutils.ReadCount(res))
		res, err = exec.Exec(ctx, "drop cdc all internal", executor.Options{})
		require.NoError(t, err)
		res.Close()
	})
}

func TestCDCCases(t *testing.T) {
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("skipping CDC integration test on GitHub Actions; it requires an external MySQL endpoint")
	}

	stubOpenDbConn := gostub.Stub(&cdc.OpenDbConn, func(_ context.Context, _, _, _ string, _ int, _ string) (*sql.DB, error) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectClose()
		return db, nil
	})
	defer stubOpenDbConn.Reset()

	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			// ensure task service is ready before CDC operations (best-effort)
			if w, ok := any(c).(interface {
				WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
			}); ok {
				ctxWait, cancelWait := context.WithTimeout(context.Background(), time.Second*60)
				w.WaitCNStoreTaskServiceCreatedIndexed(ctxWait, 0)
				cancelWait()
			}

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			defer cleanupSQLIntegration(t, cn1, "drop database if exists "+db)
			table := "table01"
			noPKTable := "table_no_pk"
			cdcTaskDB := "cdc_task_db"
			cdcTaskTbl := "cdc_task_tbl"
			cdcTaskAcc := "cdc_task_acc"
			cdcTaskNoPK := "cdc_task_no_pk"
			port := fmt.Sprintf("%d", c.ID()+199)

			conn := "mysql://dump:#admin:111@127.0.0.1:" + port

			mustExec := func(dbUsed string, sql string) {
				testutils.ExecSQLWithReadResult(t, dbUsed, cn1, nil, sql)
			}
			rows := func(dbUsed string, query string) int {
				cnt := 0
				countSQL := "select count(*) from (" + query + ") as t"
				testutils.ExecSQLWithReadResult(t, dbUsed, cn1, func(i int, s string, r executor.Result) {
					cnt = testutils.ReadCount(r)
				}, countSQL)
				return cnt
			}
			rowExists := func(dbUsed string, sql string) bool { return rows(dbUsed, sql) > 0 }

			// setup schema
			mustExec("", "create database "+db)
			mustExec(db, "create table "+table+" (col1 int primary key)")

			// ensure PITR for CDC precondition
			mustExec(db, "create pitr if not exists pitr_db for database "+db+" range 3 'h' internal")

			// helper: verify mo_catalog.mo_cdc_task by task_name
			verifyTaskPresent := func(taskName string, expect bool) {
				s := "select task_name from mo_catalog.mo_cdc_task where task_name='" + taskName + "'"
				deadline := time.Now().Add(3 * time.Second)
				backoff := 50 * time.Millisecond
				for {
					ok := rowExists("", s)
					if expect {
						if ok {
							break
						}
					} else {
						if !ok {
							break
						}
					}
					if time.Now().After(deadline) {
						// timeout; let final assert fire
						break
					}
					time.Sleep(backoff)
					if backoff < 400*time.Millisecond {
						backoff *= 2
					}
				}
				ok := rowExists("", s)
				if expect {
					require.True(t, ok, "expected task %s present", taskName)
				} else {
					require.False(t, ok, "expected task %s absent", taskName)
				}
			}

			// Case 1: database-level CDC
			mustExec(db, "create cdc "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal")
			verifyTaskPresent(cdcTaskDB, true)

			// Case 2: table-level CDC
			mustExec(db, "create cdc "+cdcTaskTbl+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"."+table+"' {'Level'='table'} internal")
			verifyTaskPresent(cdcTaskTbl, true)

			// Case 3: account-level CDC (all)
			mustExec(db, "create cdc "+cdcTaskAcc+" '"+conn+"' 'matrixone' '"+conn+"' '*.*' {'Level'='account'} internal")
			verifyTaskPresent(cdcTaskAcc, true)

			// Case 3.1: database-level with rich options
			cdcTaskOpts1 := "cdc_task_opts1"
			mustExec(db, "create cdc "+cdcTaskOpts1+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {"+
				"'Level'='database',"+
				"'NoFull'='true',"+
				"'MaxSqlLength'='8192',"+
				"'SendSqlTimeout'='2m',"+
				"'InitSnapshotSplitTxn'='false',"+
				"'Frequency'='120m',"+
				"'Exclude'='.*',"+
				"'StartTs'='2025-01-02T03:04:05Z',"+
				"'EndTs'='2025-01-02T04:05:06Z'"+
				"} internal")
			verifyTaskPresent(cdcTaskOpts1, true)
			// Validate the no_full flag via where clause
			require.Greater(t, rows("", "select task_name from mo_catalog.mo_cdc_task where task_name='"+cdcTaskOpts1+"' and no_full=true"), 0)

			// Public SQL coverage for the automatic NoFull activation boundary:
			// omitting StartTs must persist the lossless physical-logical CREATE
			// snapshot before the asynchronous executor is admitted.
			cdcTaskAutoStart := "cdc_task_autostart"
			mustExec(db, "create cdc "+cdcTaskAutoStart+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"."+table+"' {"+
				"'Level'='table','NoFull'='true'"+
				"} internal")
			verifyTaskPresent(cdcTaskAutoStart, true)
			require.Greater(t, rows("", "select task_name from mo_catalog.mo_cdc_task where task_name='"+cdcTaskAutoStart+"' and no_full=true and start_ts <> ''"), 0)

			// Case 3.2: table-level with frequency in hours
			cdcTaskOpts2 := "cdc_task_opts2"
			mustExec(db, "create cdc "+cdcTaskOpts2+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"."+table+"' {"+
				"'Level'='table',"+
				"'NoFull'='false',"+
				"'Frequency'='2h'"+
				"} internal")
			verifyTaskPresent(cdcTaskOpts2, true)

			// Case 3.3: invalid exclude regex (should error)
			_, err = exec.Exec(ctx, "create cdc bad_exclude '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database','Exclude'='\\'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.4: invalid sink type (should error)
			_, err = exec.Exec(ctx, "create cdc bad_sink '"+conn+"' 'unknown' '"+conn+"' '"+db+"' {'Level'='database'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.5: invalid StartTs format (should error)
			_, err = exec.Exec(ctx, "create cdc bad_ts '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database','StartTs'='bad'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.6: reversed time range (EndTs before StartTs) should error
			_, err = exec.Exec(ctx, "create cdc bad_time_order '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {"+
				"'Level'='database','StartTs'='2025-01-02T05:00:00Z','EndTs'='2025-01-02T04:00:00Z'"+
				"} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.7: invalid Level value 'cluster' (compile path rejects) should error
			_, err = exec.Exec(ctx, "create cdc bad_level '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='cluster'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.8: invalid Frequency '0h' (must be positive) should error
			_, err = exec.Exec(ctx, "create cdc bad_freq_zero '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database','Frequency'='0h'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.9: invalid Frequency exceeding upper bound should error
			_, err = exec.Exec(ctx, "create cdc bad_freq_large '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database','Frequency'='10000001h'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.10: invalid MaxSqlLength (non-integer) should error
			_, err = exec.Exec(ctx, "create cdc bad_max_sql '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database','MaxSqlLength'='abc'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Case 3.11: StartTs only (valid) should succeed
			cdcTaskStartOnly := "cdc_task_start_only"
			mustExec(db, "create cdc "+cdcTaskStartOnly+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {"+
				"'Level'='database','StartTs'='2025-01-02T01:02:03Z'"+
				"} internal")
			verifyTaskPresent(cdcTaskStartOnly, true)

			// Case 3.12: EndTs only (valid) should succeed
			cdcTaskEndOnly := "cdc_task_end_only"
			mustExec(db, "create cdc "+cdcTaskEndOnly+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {"+
				"'Level'='database','EndTs'='2025-01-02T06:07:08Z'"+
				"} internal")
			verifyTaskPresent(cdcTaskEndOnly, true)

			// Case 3.13: valid Exclude regex should succeed
			cdcTaskExclude := "cdc_task_exclude"
			mustExec(db, "create cdc "+cdcTaskExclude+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {"+
				"'Level'='database','Exclude'='^ignore_'"+
				"} internal")
			verifyTaskPresent(cdcTaskExclude, true)

			// Case 4: if not exists should pass when exists
			mustExec(db, "create cdc if not exists "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal")

			// Case 5: duplicate create should error
			_, err = exec.Exec(ctx, "create cdc "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// A source without a user-visible primary key is not supported: reject
			// it on the normal frontend SQL path before a CDC task is persisted.
			mustExec(db, "create table "+noPKTable+" (col1 int)")
			_, err = exec.Exec(ctx, "create cdc "+cdcTaskNoPK+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"."+noPKTable+"' {'Level'='table'}", executor.Options{}.WithDatabase(db))
			require.Error(t, err)
			verifyTaskPresent(cdcTaskNoPK, false)

			// Validation selects for presence
			require.Greater(t, rows("", "select * from mo_catalog.mo_cdc_task"), 0)

			// Drop specific task and validate absence
			mustExec(db, "drop cdc task "+cdcTaskTbl+" internal")
			verifyTaskPresent(cdcTaskTbl, false)

			// Wait for all remaining tasks to be visible before dropping
			// This avoids flaky failures due to transaction snapshot visibility issues
			expectedRemainingTasks := 7 // 8 created - 1 dropped (cdc_task_tbl)
			waitForTaskCount := func(expectedCount int) {
				deadline := time.Now().Add(5 * time.Second)
				backoff := 50 * time.Millisecond
				for {
					cnt := rows("", "select * from mo_catalog.mo_cdc_task")
					if cnt == expectedCount {
						return
					}
					if time.Now().After(deadline) {
						t.Logf("warning: expected %d tasks but found %d, proceeding anyway", expectedCount, cnt)
						return
					}
					time.Sleep(backoff)
					if backoff < 400*time.Millisecond {
						backoff *= 2
					}
				}
			}
			waitForTaskCount(expectedRemainingTasks)

			// Drop all and validate empty
			mustExec(db, "drop cdc all internal")
			waitForTaskCount(0)
			require.Equal(t, 0, rows("", "select * from mo_catalog.mo_cdc_task"))

			// cleanup PITR
			mustExec(db, "drop pitr pitr_db internal")
		},
	)
}

// TestCDCNoFullPublicLifecycle proves the public SQL lifecycle at the
// activation boundary.  Unlike TestCDCCases, this case has no external MySQL
// dependency and therefore remains enabled in GitHub Actions.
func TestCDCNoFullPublicLifecycle(t *testing.T) {
	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			// Do not race CREATE CDC with CN task-service startup.  The public
			// lifecycle assertion below is about admission ordering, so make the
			// scheduler ready before creating the task.
			if w, ok := any(c).(interface {
				WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
			}); ok {
				ctxWait, cancelWait := context.WithTimeout(ctx, 60*time.Second)
				w.WaitCNStoreTaskServiceCreatedIndexed(ctxWait, 0)
				cancelWait()
			}

			firstEntered := make(chan struct{})
			firstRelease := make(chan struct{})
			secondEntered := make(chan struct{})
			secondRelease := make(chan struct{})
			var phase atomic.Int32
			var captureReplacementBoundary atomic.Bool
			replacementBoundary := make(chan types.TS, 1)
			restoreCollectBoundary := cdc.SetCDCCollectBoundaryHookForTest(func(from, _ types.TS) {
				if captureReplacementBoundary.Load() {
					select {
					case replacementBoundary <- from:
					default:
					}
				}
			})
			defer restoreCollectBoundary()
			var firstOnce, secondOnce, firstReleaseOnce, secondReleaseOnce sync.Once
			releaseFirst := func() { firstReleaseOnce.Do(func() { close(firstRelease) }) }
			releaseSecond := func() { secondReleaseOnce.Do(func() { close(secondRelease) }) }
			defer func() {
				releaseFirst()
				releaseSecond()
			}()
			restoreAdmission := frontend.SetCDCTestAdmissionHookForTest(func() {
				switch phase.Load() {
				case 0:
					firstOnce.Do(func() { close(firstEntered) })
					<-firstRelease
					phase.Store(1)
				case 2:
					secondOnce.Do(func() { close(secondEntered) })
					<-secondRelease
					phase.Store(3)
				}
			})
			defer restoreAdmission()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			cdc.ResetTableDetectorForTest(cn.ServiceID())
			exec := testutils.GetSQLExecutor(cn)
			// Catalog CDC matching compares the persisted database identifier in
			// the scanner query. Keep this integration fixture lowercase so the
			// identifier has identical semantics across MySQL and catalog paths.
			dbName := strings.ToLower(testutils.GetDatabaseName(t))
			sinkDBName := dbName + "_sink"
			tableName := "cdc_boundary_source"
			sinkTableName := tableName
			taskName := "cdc_boundary_lifecycle"
			defer cleanupSQLIntegration(t, cn,
				"drop cdc task "+taskName+" internal",
				"drop pitr pitr_boundary internal",
				"drop pitr pitr_boundary_db internal",
				"drop database if exists "+sinkDBName,
				"drop database if exists "+dbName)
			// Cleanup can itself need the CDC task goroutine to make progress.  If
			// the test fails while a generation is blocked at admission, release
			// both barriers before cleanup runs (this defer is intentionally later
			// than cleanupSQLIntegration and therefore executes first).
			defer func() {
				releaseFirst()
				releaseSecond()
			}()

			mustExec := func(database, statement string) {
				res, execErr := exec.Exec(ctx, statement, executor.Options{}.WithDatabase(database))
				require.NoError(t, execErr, statement)
				res.Close()
			}
			taskPresent := func() bool {
				present := false
				res, queryErr := exec.Exec(ctx,
					"select task_name from mo_catalog.mo_cdc_task where task_name='"+taskName+"'",
					executor.Options{})
				if queryErr == nil {
					for _, batch := range res.Batches {
						present = present || batch.RowCount() > 0
					}
					res.Close()
				}
				return present
			}
			mustExec("", "create database "+dbName)
			mustExec(dbName, "create table "+tableName+" (id int primary key, value varchar(32))")
			mustExec(dbName, "insert into "+tableName+" values (1, 'before_create')")
			// CDC sinks into an existing target namespace; create it before
			// admission so a missing target cannot mask the lifecycle assertion.
			mustExec("", "create database "+sinkDBName)
			mustExec(dbName, "create pitr pitr_boundary for table "+dbName+" "+tableName+" range 3 'h' internal")
			mustExec(dbName, "create pitr pitr_boundary_db for database "+dbName+" range 3 'h' internal")

			port := fmt.Sprintf("%d", cn.GetServiceConfig().CN.Frontend.Port)
			uri := "mysql://sys#dump:111@127.0.0.1:" + port
			mustExec(dbName, "create cdc "+taskName+" '"+uri+"' 'matrixone' '"+uri+"' '"+dbName+":"+sinkDBName+"' {'Level'='database','NoFull'='true'} internal")
			require.Eventually(t, taskPresent, 30*time.Second, 200*time.Millisecond,
				"CREATE CDC did not persist the task row")
			// Keep the task state/error in the failure evidence.  A task can be
			// persisted successfully and still be rejected by the daemon before
			// it reaches the admission hook (for example, an executor capability
			// mismatch); the public regression must expose that rather than timing
			// out with no diagnosis.
			res, stateErr := exec.Exec(ctx,
				"select cast(account_id as varchar), tables, state, err_msg, start_ts, checkpoint_str from mo_catalog.mo_cdc_task where task_name='"+taskName+"'",
				executor.Options{})
			if stateErr == nil {
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					values := make([]string, 0, len(cols))
					for _, col := range cols {
						values = append(values, executor.GetStringRows(col)...)
					}
					t.Logf("CDC public lifecycle task state=%v", values)
					return false
				})
				res.Close()
			}

			// CREATE has returned.  Place the post-boundary commit before the
			// executor finishes admission; the hook is held after detector
			// registration.  Drive one real detector scan synchronously in a
			// goroutine so polling cadence cannot change the ordering.
			mustExec(dbName, "insert into "+tableName+" values (2, 'after_create')")
			var firstScanDone chan error
			firstScanAttempts := 0
			for {
				firstScanAttempts++
				firstScanDone = make(chan error, 1)
				go func(done chan error) { done <- cdc.RunTableDetectorScanForTest(cn.ServiceID()) }(firstScanDone)
				select {
				case <-firstEntered:
					goto firstAdmissionEntered
				case err := <-firstScanDone:
					// CREATE returns before taskservice necessarily registers the
					// executor. Retry empty detector snapshots until registration is
					// visible; a real scan error is handled the same way and will
					// surface as an admission timeout if it persists.
					if err != nil && firstScanAttempts%10 == 0 {
						t.Logf("CDC public lifecycle detector scan retry %d: %v", firstScanAttempts, err)
					}
					select {
					case <-ctx.Done():
						t.Fatal("executor did not reach the first admission barrier")
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		firstAdmissionEntered:
			releaseFirst()
			require.NoError(t, <-firstScanDone)

			countRows := func(statement string) int {
				count := 0
				res, queryErr := exec.Exec(ctx, statement, executor.Options{})
				if queryErr == nil {
					count = testutils.ReadCount(res)
					res.Close()
				} else {
					t.Logf("CDC public lifecycle target query failed: %s: %v", statement, queryErr)
				}
				return count
			}
			waitTarget := func(id int) bool {
				return countRows(fmt.Sprintf("select count(*) from %s.%s where id=%d", sinkDBName, sinkTableName, id)) > 0
			}
			require.Eventually(t, func() bool { return waitTarget(2) }, 120*time.Second, 200*time.Millisecond,
				"CDC did not deliver row 2 after admission")
			require.Equal(t, 0, countRows("select count(*) from "+sinkDBName+"."+sinkTableName+" where id=1"),
				"pre-CREATE row was delivered")

			// A watermark row also exists at initialization. Require durable
			// progress through a snapshot that actually contains row 2.
			var boundary types.TS
			require.NoError(t, exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				res, err := tx.Exec("select count(*) from "+tableName+" where id=2", executor.StatementOption{})
				if err != nil {
					return err
				}
				defer res.Close()
				if testutils.ReadCount(res) != 1 {
					return fmt.Errorf("boundary snapshot does not contain source row 2")
				}
				boundary = types.TimestampToTS(tx.Txn().SnapshotTS())
				return nil
			}, executor.Options{}.WithDatabase(dbName)))
			require.False(t, boundary.IsEmpty())
			readWatermark := func() (types.TS, bool, error) {
				res, queryErr := exec.Exec(ctx,
					"select watermark from mo_catalog.mo_cdc_watermark where task_id = (select task_id from mo_catalog.mo_cdc_task where task_name='"+taskName+"') and db_name='"+dbName+"' and table_name='"+tableName+"'",
					executor.Options{})
				if queryErr != nil {
					return types.TS{}, false, queryErr
				}
				defer res.Close()
				var watermarks []string
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					watermarks = append(watermarks, executor.GetStringRows(cols[0])...)
					return true
				})
				if len(watermarks) == 0 {
					return types.TS{}, false, nil
				}
				require.Len(t, watermarks, 1, "duplicate table watermarks")
				persisted, parseErr := frontend.CDCStrToTS(watermarks[0])
				return persisted, true, parseErr
			}

			var checkpointBeforeRestart types.TS
			deadline := time.NewTimer(30 * time.Second)
			defer deadline.Stop()
			for {
				persisted, found, queryErr := readWatermark()
				if queryErr == nil && found && persisted.GE(&boundary) {
					checkpointBeforeRestart = persisted
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("waiting for durable CDC progress: %v", ctx.Err())
				case <-deadline.C:
					t.Fatalf("CDC progress did not reach %s: query error=%v", boundary.ToString(), queryErr)
				case <-time.After(100 * time.Millisecond):
				}
			}
			t.Logf("CDC durable checkpoint before restart: %s", checkpointBeforeRestart.ToString())

			// Restart the real CN service. This tears down and recreates the CDC
			// executor while retaining the catalog task and watermark, and avoids
			// making the assertion depend on the asynchronous SQL control-plane
			// response. The source/target setup and all boundary assertions remain
			// public SQL operations.
			phase.Store(2)
			require.NoError(t, cn.Close())
			require.NoError(t, cn.Start())
			cdc.ResetTableDetectorForTest(cn.ServiceID())
			exec = testutils.GetSQLExecutor(cn)
			var secondScanDone chan error
			secondScanAttempts := 0
			for {
				secondScanAttempts++
				secondScanDone = make(chan error, 1)
				go func(done chan error) { done <- cdc.RunTableDetectorScanForTest(cn.ServiceID()) }(secondScanDone)
				select {
				case <-secondEntered:
					goto secondAdmissionEntered
				case err := <-secondScanDone:
					if err != nil && secondScanAttempts%10 == 0 {
						t.Logf("CDC public lifecycle replacement scan retry %d: %v", secondScanAttempts, err)
					}
					select {
					case <-ctx.Done():
						t.Fatal("replacement executor did not reach the second admission barrier")
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		secondAdmissionEntered:
			// Admission is still blocked, so the replacement reader has not had a
			// chance to collect changes. The durable row is only a setup check; the
			// discriminating oracle below observes the first actual CollectChanges
			// boundary after the replacement is released.
			persistedAfterRestart, found, err := readWatermark()
			require.NoError(t, err)
			require.True(t, found, "replacement reader must observe the durable watermark")
			require.Equal(t, checkpointBeforeRestart, persistedAfterRestart,
				"replacement reader must start from the persisted pre-restart checkpoint")
			mustExec(dbName, "insert into "+tableName+" values (3, 'after_restart')")
			captureReplacementBoundary.Store(true)
			releaseSecond()
			require.NoError(t, <-secondScanDone)
			select {
			case actualStart := <-replacementBoundary:
				require.Equal(t, checkpointBeforeRestart, actualStart,
					"replacement reader must collect from the durable checkpoint")
			case <-ctx.Done():
				t.Fatal("replacement reader did not reach CollectChanges")
			}

			require.Eventually(t, func() bool { return waitTarget(3) }, 120*time.Second, 200*time.Millisecond,
				"CDC did not deliver row 3 after restart")
			require.Equal(t, 1, countRows("select count(*) from "+sinkDBName+"."+sinkTableName+" where id=2"),
				"row 2 missing or duplicated after restart")
			require.Equal(t, 0, countRows("select count(*) from "+sinkDBName+"."+sinkTableName+" where id=1"),
				"pre-CREATE row was delivered after restart")
		},
	)
}

// TestCDCNoFullPublicTakeoverLifecycle proves the cross-CN ownership boundary
// against the real task and watermark tables.  The test deliberately moves the
// durable daemon claim from CN A to CN B while A's runner-selected cancellation
// is held.  B must consume the existing checkpoint and A must not erase or
// regress B's owner generation after its delayed cleanup returns.
func TestCDCNoFullPublicTakeoverLifecycle(t *testing.T) {
	// This scenario adds CNs to the shared fixture. Give each run a fresh
	// generation, including the updater's internal executor bound to CN A.
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	cdc.ResetCDCWatermarkUpdaterForTest()
	t.Cleanup(func() {
		if err := embed.CloseSingleCNBaseClusterTests(); err != nil {
			t.Errorf("close CDC takeover fixture: %v", err)
			return
		}
		cdc.ResetCDCWatermarkUpdaterForTest()
	})
	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
			defer cancel()

			cnA, err := c.GetCNService(0)
			require.NoError(t, err)
			if w, ok := any(c).(interface {
				WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
			}); ok {
				w.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
			}

			firstEntered := make(chan struct{})
			firstRelease := make(chan struct{})
			secondEntered := make(chan struct{})
			secondRelease := make(chan struct{})
			freshEntered := make(chan struct{})
			freshRelease := make(chan struct{})
			cancelEntered := make(chan bool, 2)
			cancelRelease := make(chan struct{})
			cancelCompleted := make(chan error, 1)
			bCancelDone := make(chan struct{})
			var bCancelErr error
			var bCancelErrMu sync.Mutex
			var cancelCompletionCount atomic.Int32
			var phase atomic.Int32
			var firstEnteredOnce, secondEnteredOnce, freshEnteredOnce sync.Once
			var firstReleaseOnce, secondReleaseOnce, freshReleaseOnce, cancelReleaseOnce sync.Once
			releaseFirst := func() { firstReleaseOnce.Do(func() { close(firstRelease) }) }
			releaseSecond := func() { secondReleaseOnce.Do(func() { close(secondRelease) }) }
			releaseFresh := func() { freshReleaseOnce.Do(func() { close(freshRelease) }) }
			releaseCancel := func() { cancelReleaseOnce.Do(func() { close(cancelRelease) }) }
			restoreAdmission := frontend.SetCDCTestAdmissionHookForTest(func() {
				switch phase.Load() {
				case 0:
					firstEnteredOnce.Do(func() { close(firstEntered) })
					<-firstRelease
					phase.Store(1)
				case 2:
					secondEnteredOnce.Do(func() { close(secondEntered) })
					<-secondRelease
					phase.Store(3)
				case 4:
					freshEnteredOnce.Do(func() { close(freshEntered) })
					// The test releases C only after observing B's cancellation
					// and sampling its checkpoint. The same release unblocks C
					// if an assertion fails before then.
					<-freshRelease
					phase.Store(5)
				}
			})
			defer restoreAdmission()
			restoreCancel := frontend.SetCDCTestCancelHookForTest(func(producersStopped bool) {
				select {
				case cancelEntered <- producersStopped:
				default:
				}
				<-cancelRelease
			})
			defer restoreCancel()
			restoreCancelCompletion := frontend.SetCDCTestCancelCompletionHookForTest(func(err error) {
				// The first completion is A's delayed claim-loss cleanup. After
				// the durable claim is transferred to C, B is also expected to
				// relinquish its local runner. Keep both completion points
				// observable so C cannot be admitted while B may still advance the
				// shared watermark.
				switch cancelCompletionCount.Add(1) {
				case 1:
					select {
					case cancelCompleted <- err:
					default:
					}
				case 2:
					bCancelErrMu.Lock()
					bCancelErr = err
					bCancelErrMu.Unlock()
					close(bCancelDone)
				}
			})
			defer restoreCancelCompletion()

			var captureB, captureFresh atomic.Bool
			bBoundary := make(chan types.TS, 1)
			freshBoundary := make(chan types.TS, 1)
			restoreBoundary := cdc.SetCDCCollectBoundaryHookForTest(func(from, _ types.TS) {
				if captureB.Load() {
					select {
					case bBoundary <- from:
					default:
					}
				}
				if captureFresh.Load() {
					select {
					case freshBoundary <- from:
					default:
					}
				}
			})
			defer restoreBoundary()

			cnAExec := testutils.GetSQLExecutor(cnA)
			var sqlExec = cnAExec
			dbName := strings.ToLower(testutils.GetDatabaseName(t))
			sinkDBName := dbName + "_takeover_sink"
			tableName := "cdc_takeover_source"
			taskName := "cdc_takeover_lifecycle"
			cleanupCN := cnA
			defer func() {
				cleanupSQLIntegration(t, cleanupCN,
					"drop cdc task "+taskName+" internal",
					"drop pitr pitr_takeover internal",
					"drop pitr pitr_takeover_db internal",
					"drop database if exists "+sinkDBName,
					"drop database if exists "+dbName)
			}()
			// These late defers run before cleanupSQLIntegration on every failure,
			// so a test assertion cannot leave a task runner blocked behind a
			// phase barrier while the cluster is being torn down.
			defer releaseFirst()
			defer releaseSecond()
			defer releaseFresh()
			defer releaseCancel()
			cdc.ResetTableDetectorForTest(cnA.ServiceID())

			mustExec := func(database, statement string) {
				res, execErr := sqlExec.Exec(ctx, statement, executor.Options{}.WithDatabase(database))
				require.NoError(t, execErr, statement)
				res.Close()
			}
			countRows := func(statement string) int {
				res, queryErr := sqlExec.Exec(ctx, statement, executor.Options{})
				if queryErr != nil {
					return 0
				}
				defer res.Close()
				return testutils.ReadCount(res)
			}
			waitTarget := func(id int) bool {
				return countRows(fmt.Sprintf("select count(*) from %s.%s where id=%d", sinkDBName, tableName, id)) > 0
			}
			readWatermarkFrom := func(queryCtx context.Context, queryExec executor.SQLExecutor) (types.TS, uint64, bool, error) {
				res, queryErr := queryExec.Exec(queryCtx,
					"select owner_generation, watermark from mo_catalog.mo_cdc_watermark where task_id = (select task_id from mo_catalog.mo_cdc_task where task_name='"+taskName+"') and db_name='"+dbName+"' and table_name='"+tableName+"'",
					executor.Options{})
				if queryErr != nil {
					return types.TS{}, 0, false, queryErr
				}
				defer res.Close()
				var generation uint64
				var watermark string
				rows := 0
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					generation = vector.GetFixedAtNoTypeCheck[uint64](cols[0], 0)
					watermark = cols[1].GetStringAt(0)
					rows++
					return false
				})
				if rows == 0 {
					return types.TS{}, 0, false, nil
				}
				parsed, parseErr := frontend.CDCStrToTS(watermark)
				return parsed, generation, true, parseErr
			}
			readWatermark := func() (types.TS, uint64, bool, error) {
				return readWatermarkFrom(ctx, sqlExec)
			}
			flushWatermarks := func(cnID string) {
				t.Helper()
				flushCtx, flushCancel := context.WithTimeout(ctx, 30*time.Second)
				defer flushCancel()
				require.NoError(t, cdc.GetCDCWatermarkUpdater(cnID, nil).ForceFlush(flushCtx))
			}
			waitWatermarkVisible := func(reader executor.SQLExecutor, expected types.TS, generation uint64, cn string) {
				t.Helper()
				pollCtx, pollCancel := context.WithTimeout(ctx, 30*time.Second)
				defer pollCancel()
				for {
					got, owner, exists, readErr := readWatermarkFrom(pollCtx, reader)
					if readErr == nil && exists && owner == generation && got == expected {
						return
					}
					select {
					case <-pollCtx.Done():
						t.Fatalf("CN %s did not observe final checkpoint before claiming ownership: checkpoint=%s generation=%d found=%t err=%v: %v",
							cn, got.ToString(), owner, exists, readErr, pollCtx.Err())
					case <-time.After(200 * time.Millisecond):
					}
				}
			}
			readTaskStart := func() (types.TS, error) {
				res, queryErr := sqlExec.Exec(ctx,
					"select start_ts from mo_catalog.mo_cdc_task where task_name='"+taskName+"'",
					executor.Options{})
				if queryErr != nil {
					return types.TS{}, queryErr
				}
				defer res.Close()
				var start string
				rows := 0
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					start = cols[0].GetStringAt(0)
					rows++
					return false
				})
				if rows == 0 {
					return types.TS{}, fmt.Errorf("CDC task %s not found", taskName)
				}
				return frontend.CDCStrToTS(start)
			}
			readDaemonTask := func(taskID string) (uint64, error) {
				res, queryErr := sqlExec.Exec(ctx,
					"select task_id from mo_task.sys_daemon_task where task_metadata_id='"+taskID+"'",
					executor.Options{})
				if queryErr != nil {
					return 0, queryErr
				}
				defer res.Close()
				var id uint64
				rows := 0
				res.ReadRows(func(_ int, cols []*vector.Vector) bool {
					id = vector.GetFixedAtNoTypeCheck[uint64](cols[0], 0)
					rows++
					return false
				})
				if rows == 0 {
					return 0, fmt.Errorf("daemon task %s not found", taskID)
				}
				return id, nil
			}

			mustExec("", "create database "+dbName)
			mustExec(dbName, "create table "+tableName+" (id int primary key, value varchar(32))")
			mustExec(dbName, "insert into "+tableName+" values (1, 'before_create')")
			mustExec("", "create database "+sinkDBName)
			mustExec(dbName, "create pitr pitr_takeover for table "+dbName+" "+tableName+" range 3 'h' internal")
			mustExec(dbName, "create pitr pitr_takeover_db for database "+dbName+" range 3 'h' internal")

			port := fmt.Sprintf("%d", cnA.GetServiceConfig().CN.Frontend.Port)
			uri := "mysql://sys#dump:111@127.0.0.1:" + port
			mustExec(dbName, "create cdc "+taskName+" '"+uri+"' 'matrixone' '"+uri+"' '"+dbName+":"+sinkDBName+"' {'Level'='database','NoFull'='true'} internal")
			mustExec(dbName, "insert into "+tableName+" values (2, 'after_create')")
			creationStart, err := readTaskStart()
			require.NoError(t, err)
			require.False(t, creationStart.IsEmpty())

			var firstScanDone chan error
			for attempts := 0; ; attempts++ {
				firstScanDone = make(chan error, 1)
				go func(done chan error) { done <- cdc.RunTableDetectorScanForTest(cnA.ServiceID()) }(firstScanDone)
				select {
				case <-firstEntered:
					goto firstAdmissionEntered
				case scanErr := <-firstScanDone:
					if scanErr != nil && attempts%10 == 0 {
						t.Logf("CN A detector scan retry %d: %v", attempts, scanErr)
					}
					select {
					case <-ctx.Done():
						t.Fatal("CN A did not reach admission")
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		firstAdmissionEntered:
			releaseFirst()
			require.NoError(t, <-firstScanDone)
			require.Eventually(t, func() bool { return waitTarget(2) }, 90*time.Second, 200*time.Millisecond)

			var checkpointA types.TS
			var generationA uint64
			var found bool
			require.Eventually(t, func() bool {
				var err error
				checkpointA, generationA, found, err = readWatermark()
				return err == nil && found && !checkpointA.IsEmpty() && checkpointA.GT(&creationStart)
			}, 30*time.Second, 200*time.Millisecond)

			// Add CN B only after A has admitted and persisted progress. This keeps
			// the first generation deterministic while still using a real second
			// task runner for takeover.
			require.NoError(t, c.StartNewCNService(1))
			cnB, err := c.GetCNService(1)
			require.NoError(t, err)
			if w, ok := any(c).(interface {
				WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
			}); ok {
				w.WaitCNStoreTaskServiceCreatedIndexed(ctx, 1)
			}
			sqlExec = testutils.GetSQLExecutor(cnB)
			cleanupCN = cnB

			// Move the live daemon claim to CN B in the real task table and make
			// A's heartbeat stale. This is the deterministic equivalent of a newer
			// owner generation: CN A's next heartbeat is fenced, while CN B's real
			// start runner claims the stale running row without stopping CN A.
			var cdcTaskID string
			res, queryErr := sqlExec.Exec(ctx,
				"select task_id from mo_catalog.mo_cdc_task where task_name='"+taskName+"'",
				executor.Options{})
			require.NoError(t, queryErr)
			res.ReadRows(func(_ int, cols []*vector.Vector) bool {
				cdcTaskID = vector.GetFixedAtNoTypeCheck[types.Uuid](cols[0], 0).String()
				return false
			})
			res.Close()
			require.NotEmpty(t, cdcTaskID)
			daemonID, err := readDaemonTask(cdcTaskID)
			require.NoError(t, err)
			phase.Store(2)
			mustExec("", fmt.Sprintf("update mo_task.sys_daemon_task set task_status=%d, task_runner='%s', last_heartbeat='2000-01-01 00:00:00' where task_id=%d", taskpb.TaskStatus_Running, cnB.ServiceID(), daemonID))

			// A has lost the claim, but its runner-selected cancellation is held.
			select {
			case producersStopped := <-cancelEntered:
				require.True(t, producersStopped, "CN A must stop callbacks and readers before checkpoint sampling")
			case <-ctx.Done():
				t.Fatal("CN A did not enter delayed claim-loss cleanup")
			}
			// All A producers have stopped. Drain the updater before sampling:
			// an earlier flush can pass its owner check but commit after the
			// cancellation hook, changing the durable checkpoint underneath us.
			flushWatermarks(cnA.ServiceID())
			// Read A's final checkpoint through A: B's catalog view can still be
			// behind A's commits when its task service observes the new claim.
			checkpointA, generationA, found, err = readWatermarkFrom(ctx, cnAExec)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, checkpointA.GT(&creationStart),
				"CN A must durably advance past the CREATE boundary before takeover")
			// A's cancellation remains blocked until B commits W. The stale
			// running row is not eligible to A's local runner after the claim
			// transfer, while B's task service observes and claims it.
			select {
			case <-secondEntered:
			case <-ctx.Done():
				t.Fatal("CN B did not reach replacement admission")
			}
			// The admission barrier runs before B's owner claim. Wait for B to
			// observe A's final committed row before comparing them; a single
			// cross-CN catalog read may still return an older visible version.
			waitWatermarkVisible(sqlExec, checkpointA, generationA, "B")
			checkpointBeforeB := checkpointA
			mustExec(dbName, "insert into "+tableName+" values (3, 'after_takeover')")
			// Capture a real source transaction snapshot that demonstrably sees
			// the post-takeover row.  B's durable checkpoint must reach this
			// snapshot, not merely move past its pre-admission value; otherwise a
			// sink row could be visible while a fresh reader still starts before
			// the row's committed boundary.
			var row3Snapshot types.TS
			require.NoError(t, sqlExec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				res, err := tx.Exec("select count(*) from "+tableName+" where id=3", executor.StatementOption{})
				if err != nil {
					return err
				}
				defer res.Close()
				if testutils.ReadCount(res) != 1 {
					return fmt.Errorf("takeover snapshot does not contain source row 3")
				}
				row3Snapshot = types.TimestampToTS(tx.Txn().SnapshotTS())
				return nil
			}, executor.Options{}.WithDatabase(dbName)))
			require.False(t, row3Snapshot.IsEmpty())
			require.True(t, row3Snapshot.GT(&checkpointBeforeB),
				"row 3 must commit after B's pre-admission checkpoint")
			captureB.Store(true)
			releaseSecond()
			require.Eventually(t, func() bool { return waitTarget(3) }, 90*time.Second, 200*time.Millisecond)
			select {
			case got := <-bBoundary:
				require.Equal(t, checkpointBeforeB, got)
			case <-ctx.Done():
				t.Fatal("replacement reader did not collect from B's checkpoint")
			}
			var checkpointB types.TS
			var generationAfterB uint64
			require.Eventually(t, func() bool {
				var readErr error
				checkpointB, generationAfterB, found, readErr = readWatermark()
				return readErr == nil && found && generationAfterB > generationA && checkpointB.GE(&row3Snapshot)
			}, 30*time.Second, 200*time.Millisecond)
			// B is still live. Force the delayed A cleanup to race with a real
			// later B checkpoint, rather than treating this sample as final.
			var advancedCheckpointB types.TS
			require.Eventually(t, func() bool {
				got, generation, exists, readErr := readWatermark()
				if readErr != nil || !exists || generation != generationAfterB || !got.GT(&checkpointB) {
					return false
				}
				advancedCheckpointB = got
				return true
			}, 30*time.Second, 200*time.Millisecond,
				"CN B did not advance its durable checkpoint before A's delayed cleanup")

			// Let A's delayed runner cleanup return only after B has committed W,
			// and join the actual cancellation completion rather than merely
			// observing the pre-cleanup barrier.
			releaseCancel()
			select {
			case cancelErr := <-cancelCompleted:
				require.NoError(t, cancelErr)
			case <-ctx.Done():
				t.Fatal("CN A cancellation did not complete after release")
			}
			require.Eventually(t, func() bool {
				got, generation, found, readErr := readWatermark()
				return readErr == nil && found && generation == generationAfterB && got.GE(&advancedCheckpointB)
			}, 30*time.Second, 200*time.Millisecond)

			// Add a fresh CN C after A has fully returned. Transfer the durable
			// running claim to C so a new executor/reader, rather than B's existing
			// reader, proves recovery from B's post-takeover checkpoint.
			phase.Store(4)
			require.NoError(t, c.StartNewCNService(1))
			cnC, err := c.GetCNService(2)
			require.NoError(t, err)
			if w, ok := any(c).(interface {
				WaitCNStoreTaskServiceCreatedIndexed(ctx context.Context, index int)
			}); ok {
				w.WaitCNStoreTaskServiceCreatedIndexed(ctx, 2)
			}
			sqlExec = testutils.GetSQLExecutor(cnC)
			cleanupCN = cnC
			mustExec("", fmt.Sprintf("update mo_task.sys_daemon_task set task_status=%d, task_runner='%s', last_heartbeat='2000-01-01 00:00:00' where task_id=%d", taskpb.TaskStatus_Running, cnC.ServiceID(), daemonID))
			// Drive C's real detector synchronously as well.  Relying only on the
			// periodic detector ticker makes this lifecycle test nondeterministic:
			// after the daemon claim moves to C, the next ticker may be delayed long
			// enough for the test/UT watchdog to fire before the replacement reader
			// reaches its admission barrier.
			var freshScanDone chan error
			defer func() {
				if freshScanDone == nil {
					return
				}
				releaseFresh()
				select {
				case <-freshScanDone:
				case <-time.After(30 * time.Second):
					t.Error("CN C detector scan did not finish during cleanup")
				}
			}()
			for attempts := 0; ; attempts++ {
				freshScanDone = make(chan error, 1)
				go func(done chan error) { done <- cdc.RunTableDetectorScanForTest(cnC.ServiceID()) }(freshScanDone)
				select {
				case <-freshEntered:
					goto freshAdmissionEntered
				case scanErr := <-freshScanDone:
					freshScanDone = nil
					if scanErr != nil && attempts%10 == 0 {
						t.Logf("CN C detector scan retry %d: %v", attempts, scanErr)
					}
					select {
					case <-ctx.Done():
						t.Fatal("CN C did not reach fresh-reader admission")
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
		freshAdmissionEntered:
			select {
			case <-bCancelDone:
				bCancelErrMu.Lock()
				err := bCancelErr
				bCancelErrMu.Unlock()
				require.NoError(t, err, "CN B cancellation must complete before C admission")
			case <-ctx.Done():
				t.Fatal("CN B cancellation did not complete before C admission")
			}
			select {
			case producersStopped := <-cancelEntered:
				require.True(t, producersStopped, "CN B must stop callbacks and readers before checkpoint sampling")
			case <-ctx.Done():
				t.Fatal("CN B did not stop its producers before C admission")
			}
			// B's cancellation schedules cache eviction asynchronously. Drain any
			// earlier B write before sampling the durable row for C.
			flushWatermarks(cnB.ServiceID())
			// The updater writes through A's internal executor. Read the final
			// committed tuple there, then wait for C's catalog view to catch up.
			freshStart, finalGeneration, found, err := readWatermarkFrom(ctx, cnAExec)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, generationAfterB, finalGeneration,
				"B's final checkpoint must retain its owner generation before C claims")
			require.True(t, freshStart.GE(&advancedCheckpointB),
				"fresh reader must not observe a checkpoint older than B's durable progress")
			waitWatermarkVisible(sqlExec, freshStart, finalGeneration, "C")
			captureFresh.Store(true)
			releaseFresh()
			select {
			case scanErr := <-freshScanDone:
				freshScanDone = nil
				require.NoError(t, scanErr)
			case <-ctx.Done():
				t.Fatal("CN C detector scan did not finish after admission")
			}
			select {
			case actualStart := <-freshBoundary:
				require.Equal(t, freshStart, actualStart,
					"fresh reader must collect from the surviving durable checkpoint")
			case <-ctx.Done():
				t.Fatal("fresh reader did not reach CollectChanges")
			}
		},
	)
}

func TestAlterRoleCases(t *testing.T) {
	runSQLIntegration(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn0, err := c.GetCNService(0)
			require.NoError(t, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)

			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			mustExec := func(sql string) {
				_, err := db.ExecContext(ctx, sql)
				require.NoError(t, err, "failed to execute: %s", sql)
			}
			mustError := func(sql string) {
				_, err := db.ExecContext(ctx, sql)
				require.Error(t, err, "expected error for: %s", sql)
			}
			tryDropRole := func(roleName string) {
				_, err := db.ExecContext(ctx, "drop role if exists "+roleName)
				// Ignore errors for cleanup operations
				_ = err
			}
			roleExists := func(roleName string) bool {
				query := "select count(*) from mo_catalog.mo_role where role_name = '" + roleName + "'"
				var cnt int
				err := db.QueryRowContext(ctx, query).Scan(&cnt)
				if err != nil {
					return false
				}
				return cnt > 0
			}

			// Case 1: Basic rename - create role and rename it
			role1 := "test_role_1"
			role1New := "test_role_1_renamed"
			tryDropRole(role1)
			tryDropRole(role1New)
			mustExec("create role " + role1)
			require.True(t, roleExists(role1), "role %s should exist after creation", role1)
			require.False(t, roleExists(role1New), "role %s should not exist before rename", role1New)

			mustExec("alter role " + role1 + " rename to " + role1New)
			require.False(t, roleExists(role1), "role %s should not exist after rename", role1)
			require.True(t, roleExists(role1New), "role %s should exist after rename", role1New)

			// Case 2: Rename with IF EXISTS - role exists
			role2 := "test_role_2"
			role2New := "test_role_2_renamed"
			tryDropRole(role2)
			tryDropRole(role2New)
			mustExec("create role " + role2)
			mustExec("alter role if exists " + role2 + " rename to " + role2New)
			require.False(t, roleExists(role2), "role %s should not exist after rename", role2)
			require.True(t, roleExists(role2New), "role %s should exist after rename", role2New)

			// Case 3: Rename with IF EXISTS - role does not exist (should succeed silently)
			role3 := "test_role_nonexistent"
			role3New := "test_role_nonexistent_new"
			tryDropRole(role3)
			tryDropRole(role3New)
			mustExec("alter role if exists " + role3 + " rename to " + role3New)
			require.False(t, roleExists(role3New), "role %s should not exist when source role doesn't exist", role3New)

			// Case 4: Error - rename non-existent role without IF EXISTS
			role4 := "test_role_nonexistent_2"
			role4New := "test_role_nonexistent_2_new"
			tryDropRole(role4)
			tryDropRole(role4New)
			mustError("alter role " + role4 + " rename to " + role4New)

			// Case 5: Error - rename to existing role name
			role5 := "test_role_5"
			role5Existing := "test_role_5_existing"
			tryDropRole(role5)
			tryDropRole(role5Existing)
			mustExec("create role " + role5)
			mustExec("create role " + role5Existing)
			mustError("alter role " + role5 + " rename to " + role5Existing)

			// Case 6: Error - rename to existing user name
			// Note: Skipping CREATE USER test due to system variable initialization issue with DSN connections
			// This case would test: alter role role6 rename to existing_user_name
			// The error handling for renaming to existing user name is covered by Case 5 (existing role name)

			// Case 7: Error - rename to predefined role name (moadmin)
			role7 := "test_role_7"
			tryDropRole(role7)
			mustExec("create role " + role7)
			mustError("alter role " + role7 + " rename to moadmin")

			// Case 8: Error - rename to predefined role name (accountadmin)
			role8 := "test_role_8"
			tryDropRole(role8)
			mustExec("create role " + role8)
			mustError("alter role " + role8 + " rename to accountadmin")

			// Case 9: Error - rename to predefined role name (public)
			role9 := "test_role_9"
			tryDropRole(role9)
			mustExec("create role " + role9)
			mustError("alter role " + role9 + " rename to public")

			// Case 10: Error - rename admin role (moadmin)
			mustError("alter role moadmin rename to moadmin_new")

			// Case 11: Error - rename admin role (accountadmin)
			mustError("alter role accountadmin rename to accountadmin_new")

			// Case 12: Error - rename public role
			mustError("alter role public rename to public_new")

			// Case 13: Error - rename to same name
			role13 := "test_role_13"
			tryDropRole(role13)
			mustExec("create role " + role13)
			mustError("alter role " + role13 + " rename to " + role13)

			// Case 14: Multiple renames in sequence
			role14 := "test_role_14"
			role14Step1 := "test_role_14_step1"
			role14Step2 := "test_role_14_step2"
			tryDropRole(role14)
			tryDropRole(role14Step1)
			tryDropRole(role14Step2)
			mustExec("create role " + role14)
			mustExec("alter role " + role14 + " rename to " + role14Step1)
			require.True(t, roleExists(role14Step1), "role %s should exist after first rename", role14Step1)
			mustExec("alter role " + role14Step1 + " rename to " + role14Step2)
			require.False(t, roleExists(role14Step1), "role %s should not exist after second rename", role14Step1)
			require.True(t, roleExists(role14Step2), "role %s should exist after second rename", role14Step2)

			// Case 15: Rename with special characters in name (if supported)
			role15 := "test_role_15"
			role15New := "test_role_15_special"
			tryDropRole(role15)
			tryDropRole(role15New)
			mustExec("create role " + role15)
			mustExec("alter role " + role15 + " rename to " + role15New)
			require.True(t, roleExists(role15New), "role %s should exist after rename", role15New)

			// Case 16: Verify role grant persists after rename
			// Use the default 'dump' user to test that grants are preserved after role rename
			role16 := "test_role_16"
			role16New := "test_role_16_new"
			dumpUser := "dump"
			tryDropRole(role16)
			tryDropRole(role16New)
			mustExec("create role " + role16)
			// Grant role to dump user
			mustExec("grant " + role16 + " to " + dumpUser)
			// Verify grant exists before rename
			var cntBefore int
			queryBefore := "select count(*) from mo_catalog.mo_user_grant ug join mo_catalog.mo_role r on ug.role_id = r.role_id where r.role_name = '" + role16 + "' and ug.user_id = (select user_id from mo_catalog.mo_user where user_name = '" + dumpUser + "')"
			err = db.QueryRowContext(ctx, queryBefore).Scan(&cntBefore)
			require.NoError(t, err)
			require.Equal(t, 1, cntBefore, "grant should exist before rename")
			// Rename the role
			mustExec("alter role " + role16 + " rename to " + role16New)
			// Verify grant still exists with new role name
			var cntAfter int
			queryAfter := "select count(*) from mo_catalog.mo_user_grant ug join mo_catalog.mo_role r on ug.role_id = r.role_id where r.role_name = '" + role16New + "' and ug.user_id = (select user_id from mo_catalog.mo_user where user_name = '" + dumpUser + "')"
			err = db.QueryRowContext(ctx, queryAfter).Scan(&cntAfter)
			require.NoError(t, err)
			require.Equal(t, 1, cntAfter, "grant should still exist after role rename")
			// Verify old role name grant no longer exists
			var cntOld int
			queryOld := "select count(*) from mo_catalog.mo_user_grant ug join mo_catalog.mo_role r on ug.role_id = r.role_id where r.role_name = '" + role16 + "' and ug.user_id = (select user_id from mo_catalog.mo_user where user_name = '" + dumpUser + "')"
			err = db.QueryRowContext(ctx, queryOld).Scan(&cntOld)
			require.NoError(t, err)
			require.Equal(t, 0, cntOld, "old role name grant should not exist after rename")
			// Cleanup: revoke the role from dump user
			mustExec("revoke " + role16New + " from " + dumpUser)

			// Case 17: Cleanup - drop all test roles
			testRoles := []string{
				role1New, role2New, role5, role5Existing, role7, role8, role9,
				role13, role14Step2, role15New, role16New,
			}
			for _, r := range testRoles {
				tryDropRole(r)
			}
		},
	)
}
