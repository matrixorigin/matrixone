// Copyright 2026 Matrix Origin
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

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

// TestIssue28299ReplaceValuesOrder blocks both scalar-subquery branches on
// independent fault-point barriers. Releasing the syntactically later branch
// before the earlier one proves that REPLACE restores VALUES order rather than
// using UNION ALL arrival order for duplicate primary and unique keys.
func TestIssue28299ReplaceValuesOrder(t *testing.T) {
	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}

	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(4)

		name := fmt.Sprintf("issue_28299_%d", time.Now().UnixNano())
		execSQLRequire(t, ctx, db, "create database "+name)
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists "+name)
		execSQLRequire(t, ctx, db, "create table "+name+".target (id int primary key, uk int unique, result int)")
		execSQLRequire(t, ctx, db, "create table "+name+".source (v int)")
		execSQLRequire(t, ctx, db, "insert into "+name+".source values (10), (20), (30), (40)")

		runner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer runner.Close()
		_, err = runner.ExecContext(ctx, "use "+name)
		require.NoError(t, err)

		type execResult struct {
			result sql.Result
			err    error
		}
		runGatedReplace := func(sqlTemplate, expectedSQL string, expected int) {
			suffix := fmt.Sprintf("%d", time.Now().UnixNano())
			firstBarrier := "issue28299_first_" + suffix
			laterBarrier := "issue28299_later_" + suffix
			firstWaiters := "issue28299_first_waiters_" + suffix
			laterWaiters := "issue28299_later_waiters_" + suffix
			firstNotify := "issue28299_first_notify_" + suffix
			laterNotify := "issue28299_later_notify_" + suffix
			points := []string{firstBarrier, laterBarrier, firstWaiters, laterWaiters, firstNotify, laterNotify}
			defer func() {
				for _, point := range points {
					_, _ = fault.RemoveFaultPoint(context.Background(), point)
				}
			}()
			require.NoError(t, fault.AddFaultPoint(ctx, firstBarrier, ":::", "wait", 0, "", false))
			require.NoError(t, fault.AddFaultPoint(ctx, laterBarrier, ":::", "wait", 0, "", false))
			require.NoError(t, fault.AddFaultPoint(ctx, firstWaiters, ":::", "getwaiters", 0, firstBarrier, false))
			require.NoError(t, fault.AddFaultPoint(ctx, laterWaiters, ":::", "getwaiters", 0, laterBarrier, false))
			require.NoError(t, fault.AddFaultPoint(ctx, firstNotify, ":::", "notifyall", 0, firstBarrier, false))
			require.NoError(t, fault.AddFaultPoint(ctx, laterNotify, ":::", "notifyall", 0, laterBarrier, false))

			sqlText := fmt.Sprintf(sqlTemplate, firstBarrier, laterBarrier)
			resultCh := make(chan execResult, 1)
			go func() {
				result, err := runner.ExecContext(ctx, sqlText)
				resultCh <- execResult{result: result, err: err}
			}()

			require.Eventually(t, func() bool {
				first, _, firstOK := fault.TriggerFault(firstWaiters)
				later, _, laterOK := fault.TriggerFault(laterWaiters)
				return firstOK && laterOK && first == 1 && later == 1
			}, 30*time.Second, 10*time.Millisecond, "both VALUES branches must reach their barriers")
			_, _, ok := fault.TriggerFault(laterNotify)
			require.True(t, ok)
			require.Eventually(t, func() bool {
				first, _, firstOK := fault.TriggerFault(firstWaiters)
				later, _, laterOK := fault.TriggerFault(laterWaiters)
				return firstOK && laterOK && first == 1 && later == 0
			}, 30*time.Second, 10*time.Millisecond, "later VALUES branch must finish before the earlier branch is released")
			select {
			case result := <-resultCh:
				require.FailNow(t, "REPLACE returned while the earlier VALUES branch was blocked", "error: %v", result.err)
			default:
			}
			_, _, ok = fault.TriggerFault(firstNotify)
			require.True(t, ok)

			var result execResult
			select {
			case result = <-resultCh:
			case <-time.After(30 * time.Second):
				t.Fatal("REPLACE did not finish after both VALUES branches were released")
			}
			require.NoError(t, result.err)
			affected, err := result.result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), affected)
			var got int
			require.NoError(t, runner.QueryRowContext(ctx, expectedSQL).Scan(&got))
			require.Equal(t, expected, got)
		}

		execSQLRequire(t, ctx, db, "truncate table "+name+".target")
		runGatedReplace(
			"replace into "+name+".target values (1, 101, (select 10 + trigger_fault_point('%s'))), (1, 102, (select 20 + trigger_fault_point('%s')))",
			"select result from "+name+".target where id = 1 and uk = 102", 20)

		execSQLRequire(t, ctx, db, "truncate table "+name+".target")
		runGatedReplace(
			"replace into "+name+".target values (10, 201, (select 30 + trigger_fault_point('%s'))), (11, 201, (select 40 + trigger_fault_point('%s')))",
			"select result from "+name+".target where id = 11 and uk = 201", 40)

		execSQLRequire(t, ctx, db, "truncate table "+name+".target")
		execSQLRequire(t, ctx, db, "insert into "+name+".target values (99, 999, 999)")
		_, err = runner.ExecContext(ctx,
			"replace into "+name+".target values (99, 998, (select max(v) from source)), (100, 997, (select v from source))")
		require.Error(t, err)
		var id, uk, result int
		require.NoError(t, runner.QueryRowContext(ctx, "select id, uk, result from target").Scan(&id, &uk, &result))
		require.Equal(t, []int{99, 999, 999}, []int{id, uk, result})

		// Cancellation while both source branches are blocked must unwind without
		// writing either row or leaving a branch behind.
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		firstBarrier := "issue28299_cancel_first_" + suffix
		laterBarrier := "issue28299_cancel_later_" + suffix
		firstWaiters := "issue28299_cancel_first_waiters_" + suffix
		laterWaiters := "issue28299_cancel_later_waiters_" + suffix
		cancelPoints := []string{firstBarrier, laterBarrier, firstWaiters, laterWaiters}
		defer func() {
			for _, point := range cancelPoints {
				_, _ = fault.RemoveFaultPoint(context.Background(), point)
			}
		}()
		require.NoError(t, fault.AddFaultPoint(ctx, firstBarrier, ":::", "wait", 0, "", false))
		require.NoError(t, fault.AddFaultPoint(ctx, laterBarrier, ":::", "wait", 0, "", false))
		require.NoError(t, fault.AddFaultPoint(ctx, firstWaiters, ":::", "getwaiters", 0, firstBarrier, false))
		require.NoError(t, fault.AddFaultPoint(ctx, laterWaiters, ":::", "getwaiters", 0, laterBarrier, false))
		var runnerID uint64
		require.NoError(t, runner.QueryRowContext(ctx, "select connection_id()").Scan(&runnerID))
		cancelResult := make(chan error, 1)
		go func() {
			_, cancelErr := runner.ExecContext(ctx, fmt.Sprintf(
				"replace into target values (101, 901, (select trigger_fault_point('%s'))), (102, 902, (select trigger_fault_point('%s')))",
				firstBarrier, laterBarrier))
			cancelResult <- cancelErr
		}()
		require.Eventually(t, func() bool {
			first, _, firstOK := fault.TriggerFault(firstWaiters)
			later, _, laterOK := fault.TriggerFault(laterWaiters)
			return firstOK && laterOK && first == 1 && later == 1
		}, 30*time.Second, 10*time.Millisecond, "canceled source branches must first reach both barriers")
		_, err = db.ExecContext(ctx, fmt.Sprintf("kill query %d", runnerID))
		require.NoError(t, err)
		// The synthetic fault WAIT action itself is not context-aware. Remove its
		// barriers after KILL QUERY so the canceled operators can return and prove
		// that their merge/sort cleanup does not retain either source branch.
		_, _ = fault.RemoveFaultPoint(ctx, firstBarrier)
		_, _ = fault.RemoveFaultPoint(ctx, laterBarrier)
		select {
		case cancelErr := <-cancelResult:
			require.Error(t, cancelErr)
		case <-time.After(30 * time.Second):
			t.Fatal("canceled REPLACE did not unwind")
		}
		require.Eventually(t, func() bool {
			first, _, firstOK := fault.TriggerFault(firstWaiters)
			later, _, laterOK := fault.TriggerFault(laterWaiters)
			return firstOK && laterOK && first == 0 && later == 0
		}, 30*time.Second, 10*time.Millisecond, "cancellation must remove both blocked VALUES branches")

		// Verify from a fresh session that the server-side pipeline fully unwound
		// and did not publish a partial write.
		_ = runner.Close()
		runner, err = db.Conn(ctx)
		require.NoError(t, err)
		defer runner.Close()
		_, err = runner.ExecContext(ctx, "use "+name)
		require.NoError(t, err)
		require.NoError(t, runner.QueryRowContext(ctx, "select id, uk, result from target").Scan(&id, &uk, &result))
		require.Equal(t, []int{99, 999, 999}, []int{id, uk, result})

		// A large VALUES list with one scalar subquery must compile and execute as
		// one subquery branch plus one compact literal RowsetData branch.
		execSQLRequire(t, ctx, db, "truncate table "+name+".target")
		var rows strings.Builder
		for row := 0; row < 1000; row++ {
			if row > 0 {
				rows.WriteByte(',')
			}
			if row == 500 {
				fmt.Fprintf(&rows, "(%d, %d, (select max(v) from source))", row+1000, row+2000)
			} else {
				fmt.Fprintf(&rows, "(%d, %d, %d)", row+1000, row+2000, row)
			}
		}
		largeResult, err := runner.ExecContext(ctx, "replace into target values "+rows.String())
		require.NoError(t, err)
		affected, err := largeResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1000), affected)
		var count int
		require.NoError(t, runner.QueryRowContext(ctx, "select count(*) from target").Scan(&count))
		require.Equal(t, 1000, count)
	})
}
