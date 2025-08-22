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

package ddl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestCreateAndDropPitr(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
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
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
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
				require.Greater(t, cnt, 0)
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
				require.Greater(t, tc, 0)
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

func TestCDCCases(t *testing.T) {
	embed.RunBaseClusterTests(
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
			table := "table01"
			cdcTaskDB := "cdc_task_db"
			cdcTaskTbl := "cdc_task_tbl"
			cdcTaskAcc := "cdc_task_acc"
			port := fmt.Sprintf("%d", c.ID()+199)

			conn := "mysql://dump:#admin:111@127.0.0.1:" + port

			mustExec := func(sql string, opts executor.Options) {
				res, err := exec.Exec(ctx, sql, opts)
				require.NoError(t, err, "sql: %s", sql)
				res.Close()
			}
			rows := func(sql string, opts executor.Options) int {
				res, err := exec.Exec(ctx, sql, opts)
				require.NoError(t, err, "sql: %s", sql)
				cnt := 0
				for _, b := range res.Batches {
					cnt += b.RowCount()
				}
				res.Close()
				return cnt
			}
			rowExists := func(sql string, opts executor.Options) bool { return rows(sql, opts) > 0 }

			// setup schema
			mustExec("create database "+db, executor.Options{})
			mustExec("create table "+table+" (col1 int)", executor.Options{}.WithDatabase(db))

			// ensure PITR for CDC precondition
			mustExec("create pitr if not exists pitr_db for database "+db+" range 2 'h' internal", executor.Options{}.WithDatabase(db))

			// helper: verify mo_catalog.mo_cdc_task by task_name
			verifyTaskPresent := func(taskName string, expect bool) {
				s := "select task_name from mo_catalog.mo_cdc_task where task_name='" + taskName + "'"
				ok := rowExists(s, executor.Options{})
				if expect {
					require.True(t, ok, "expected task %s present", taskName)
				} else {
					require.False(t, ok, "expected task %s absent", taskName)
				}
			}

			// Case 1: database-level CDC
			mustExec("create cdc "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal", executor.Options{}.WithDatabase(db))
			verifyTaskPresent(cdcTaskDB, true)

			// Case 2: table-level CDC
			mustExec("create cdc "+cdcTaskTbl+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"."+table+"' {'Level'='table'} internal", executor.Options{}.WithDatabase(db))
			verifyTaskPresent(cdcTaskTbl, true)

			// Case 3: account-level CDC (all)
			mustExec("create cdc "+cdcTaskAcc+" '"+conn+"' 'matrixone' '"+conn+"' '*.*' {'Level'='account'} internal", executor.Options{}.WithDatabase(db))
			verifyTaskPresent(cdcTaskAcc, true)

			// Case 4: if not exists should pass when exists
			mustExec("create cdc if not exists "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal", executor.Options{}.WithDatabase(db))

			// Case 5: duplicate create should error
			_, err = exec.Exec(ctx, "create cdc "+cdcTaskDB+" '"+conn+"' 'matrixone' '"+conn+"' '"+db+"' {'Level'='database'} internal", executor.Options{}.WithDatabase(db))
			require.Error(t, err)

			// Validation selects for presence
			require.Greater(t, rows("select * from mo_catalog.mo_cdc_task", executor.Options{}), 0)

			// Drop specific task and validate absence
			mustExec("drop cdc task "+cdcTaskTbl+" internal", executor.Options{}.WithDatabase(db))
			verifyTaskPresent(cdcTaskTbl, false)

			// Drop all and validate empty
			mustExec("drop cdc all internal", executor.Options{}.WithDatabase(db))
			require.Equal(t, rows("select * from mo_catalog.mo_cdc_task", executor.Options{}), 0)

			// cleanup PITR
			mustExec("drop pitr pitr_db internal", executor.Options{}.WithDatabase(db))
		},
	)
}
