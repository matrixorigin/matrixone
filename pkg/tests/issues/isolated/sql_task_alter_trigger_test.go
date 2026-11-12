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

package isolated

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbtask "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskAlterAfterTriggerOnRealStorage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithTestAdmissionContext(ctx))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	peer, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, waitSystemBootstrap(ctx, db))
	require.NoError(t, waitSQLTaskReady(ctx, db))
	peerDB := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
	t.Cleanup(func() { require.NoError(t, peerDB.Close()) })
	require.NoError(t, waitSystemBootstrap(ctx, peerDB))
	require.NoError(t, waitSQLTaskReady(ctx, peerDB))
	hakeeper, err := logservice.NewCNHAKeeperClient(ctx, cn.GetServiceConfig().CN.UUID, cn.GetServiceConfig().HAKeeperClient)
	require.NoError(t, err)
	state, err := hakeeper.GetClusterState(ctx)
	require.NoError(t, err)
	require.NoError(t, hakeeper.Close())
	config := mysql.NewConfig()
	config.User, config.Passwd = state.TaskTableUser.Username, state.TaskTableUser.Password
	config.Net, config.Addr, config.DBName = "tcp", fmt.Sprintf("127.0.0.1:%d", peer.GetServiceConfig().CN.Frontend.Port), "mo_task"
	config.ParseTime = true
	taskDB, err := sql.Open("mysql", config.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, taskDB.Close()) })
	require.NoError(t, taskDB.PingContext(ctx))
	execSQLRequire(t, ctx, db, "create database alter_trigger_test")
	execSQLRequire(t, ctx, db, "use alter_trigger_test")
	first, ok := cn.RawService().(testutil.TaskServiceGetter).GetTaskService()
	require.True(t, ok)
	second, ok := peer.RawService().(testutil.TaskServiceGetter).GetTaskService()
	require.True(t, ok)
	execSQLRequire(t, ctx, db, "create task alter_seed schedule '0 0 0 1 1 *' timezone 'UTC' as begin select 1; end")
	seed, err := first.GetStorage().QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, 0), taskservice.WithTaskName(taskservice.EQ, "alter_seed"))
	require.NoError(t, err)
	require.Len(t, seed, 1)
	template := seed[0]
	for _, tc := range []struct {
		name   string
		action taskservice.SQLTaskAlterAction
	}{
		{"suspend", taskservice.SQLTaskSuspend}, {"schedule", taskservice.SQLTaskSetSchedule},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "alter_" + tc.name
			execSQLRequire(t, ctx, db, "create task "+name+" schedule '0 0 0 1 1 *' timezone 'UTC' as begin select 1; end")
			tasks, err := first.GetStorage().QuerySQLTask(ctx, taskservice.WithAccountID(taskservice.EQ, 0), taskservice.WithTaskName(taskservice.EQ, name))
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			observed := tasks[0]
			trigger := observed
			trigger.TriggerCount++
			spec := &pbtask.SQLTaskContext{TaskId: observed.TaskID, TaskName: observed.TaskName, AccountId: observed.AccountID, TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: trigger.TriggerCount, DatabaseName: observed.DatabaseName, SQLBody: observed.SQLBody}
			async := pbtask.AsyncTask{Metadata: taskservice.BuildSQLTaskMetadata(spec), ParentTaskID: fmt.Sprintf("sql-task:%d", observed.TaskID), Status: pbtask.TaskStatus_Created}
			n, err := second.GetStorage().TriggerSQLTask(ctx, trigger, async)
			require.NoError(t, err)
			require.Equal(t, 2, n)
			updated := observed
			updated.CronExpr, updated.Timezone, updated.NextFireTime = "0 0 0 2 1 *", "UTC", time.Now().Add(24*time.Hour).UnixMilli()
			n, err = first.GetStorage().AlterSQLTask(ctx, observed, updated, tc.action)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			tasks, err = first.GetStorage().QuerySQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, observed.TaskID))
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Equal(t, trigger.TriggerCount+1, tasks[0].TriggerCount)
			require.Equal(t, tasks[0].TriggerCount, tasks[0].ScheduleGeneration)
			if tc.action == taskservice.SQLTaskSuspend {
				require.False(t, tasks[0].Enabled)
			} else {
				require.Equal(t, updated.CronExpr, tasks[0].CronExpr)
			}
			n, err = first.GetStorage().AlterSQLTask(ctx, observed, updated, tc.action)
			require.NoError(t, err)
			require.Zero(t, n)
			// Restore enabled state without changing generation, so only the stale
			// generation can reject the scheduler's next otherwise-valid firing.
			if tc.action == taskservice.SQLTaskSuspend {
				resumed := tasks[0]
				resumed.NextFireTime = time.Now().Add(24 * time.Hour).UnixMilli()
				n, err = first.GetStorage().AlterSQLTask(ctx, tasks[0], resumed, taskservice.SQLTaskResume)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			}
			trigger.TriggerCount = tasks[0].TriggerCount + 1
			spec.TriggerCount = trigger.TriggerCount
			async.Metadata = taskservice.BuildSQLTaskMetadata(spec)
			n, err = second.GetStorage().TriggerSQLTask(ctx, trigger, async)
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
	// Hold a scheduler-shaped transaction open after both its counter update
	// and firing insert. Observe ALTER waiting on that row (or returning a
	// conflict) before deciding whether the writer commits or rolls back.
	var tableID uint64
	require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_task' and relname='sql_task'").Scan(&tableID))
	for _, tc := range []struct {
		name   string
		commit bool
		cancel bool
	}{{name: "overlap_commit", commit: true}, {name: "overlap_rollback"}, {name: "overlap_cancel", cancel: true}} {
		name := tc.name
		t.Run(name, func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create task "+name+" schedule '0 0 0 1 1 *' timezone 'UTC' as begin select 1; end")
			rows, err := first.GetStorage().QuerySQLTask(ctx, taskservice.WithTaskName(taskservice.EQ, name))
			require.NoError(t, err)
			require.Len(t, rows, 1)
			observed := rows[0]
			tx, err := taskDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			result, err := tx.ExecContext(ctx, "update mo_task.sql_task set trigger_count=trigger_count+1 where task_id=? and schedule_generation=? and trigger_count=?", observed.TaskID, observed.ScheduleGeneration, observed.TriggerCount)
			require.NoError(t, err)
			n, err := result.RowsAffected()
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
			firingID := name + "_firing"
			// A paused synthetic firing is neither scheduled nor truncated while
			// the test checks the transaction's commit/rollback outcome.
			_, err = tx.ExecContext(ctx, "insert into mo_task.sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?,0,?, '{}',?,3,'',0,0,?,?)", firingID, []byte{}, fmt.Sprintf("sql-task:%d", observed.TaskID), time.Now().UnixMilli(), time.Now().UnixMilli())
			require.NoError(t, err)
			type alterResult struct {
				n   int
				err error
			}
			done := make(chan alterResult, 1)
			alterCtx, stopAlter := context.WithCancel(ctx)
			defer stopAlter()
			go func() {
				n, err := first.GetStorage().AlterSQLTask(alterCtx, observed, observed, taskservice.SQLTaskSuspend)
				done <- alterResult{n, err}
			}()
			var early *alterResult
			require.Eventually(t, func() bool {
				select {
				case r := <-done:
					early = &r
					return true
				default:
				}
				for _, service := range []string{cn.ServiceID(), peer.ServiceID()} {
					ls := lockservice.GetLockServiceByServiceID(service)
					if ls != nil && sqlTaskRowHasWaiter(ls, tableID, observed.TaskID) {
						return true
					}
				}
				return false
			}, 15*time.Second, 20*time.Millisecond, "ALTER did not enter the overlapping transaction")
			if tc.cancel {
				require.Nil(t, early, "ALTER must be waiting before cancellation")
				stopAlter()
				select {
				case r := <-done:
					early = &r
					require.ErrorIs(t, r.err, context.Canceled)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if tc.commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}
			var outcome alterResult
			if early != nil {
				outcome = *early
			} else {
				select {
				case outcome = <-done:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if outcome.err == nil {
				require.Equal(t, 1, outcome.n)
			}
			var count int
			require.NoError(t, taskDB.QueryRowContext(ctx, "select count(*) from mo_task.sys_async_task where task_metadata_id=?", firingID).Scan(&count))
			if tc.commit {
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, count)
			}
			if tc.cancel {
				// The server may still commit after the client returns. The
				// writer rollback above is known, but ALTER's outcome is not.
				return
			}
			current, err := first.GetStorage().QuerySQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, observed.TaskID))
			require.NoError(t, err)
			require.Len(t, current, 1)
			if outcome.err == nil {
				require.False(t, current[0].Enabled)
				require.Equal(t, current[0].TriggerCount, current[0].ScheduleGeneration)
				if tc.commit {
					require.Greater(t, current[0].TriggerCount, observed.TriggerCount+1)
				}
			} else {
				var sqlErr *mysql.MySQLError
				require.ErrorAs(t, outcome.err, &sqlErr)
				require.Contains(t, sqlErr.Message, "conflict")
				if tc.commit {
					require.Equal(t, observed.TriggerCount+1, current[0].TriggerCount)
				} else {
					require.Equal(t, observed.TriggerCount, current[0].TriggerCount)
				}
			}
		})
	}
	// Exercise the actual signed-BIGINT boundary through SQL storage.
	template.TaskID, template.TaskName, template.TriggerCount = 0, "alter_limit", math.MaxInt64-1
	n, err := first.GetStorage().AddSQLTask(ctx, template)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	rows, err := first.GetStorage().QuerySQLTask(ctx, taskservice.WithTaskName(taskservice.EQ, template.TaskName))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	n, err = first.GetStorage().AlterSQLTask(ctx, rows[0], rows[0], taskservice.SQLTaskSuspend)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	rows, err = first.GetStorage().QuerySQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, rows[0].TaskID))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(math.MaxInt64), rows[0].TriggerCount)
	require.Equal(t, rows[0].TriggerCount, rows[0].ScheduleGeneration)
	_, err = first.GetStorage().AlterSQLTask(ctx, rows[0], rows[0], taskservice.SQLTaskSuspend)
	require.ErrorIs(t, err, taskservice.ErrSQLTaskSequenceExhausted)
}

func sqlTaskRowHasWaiter(ls lockservice.LockService, tableID, taskID uint64) bool {
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt64(int64(taskID)) // sql_task.task_id is a signed BIGINT.
	key := packer.Bytes()
	found := false
	ls.IterLocks(func(table uint64, keys [][]byte, lock lockservice.Lock) bool {
		if table != tableID {
			return true
		}
		for _, current := range keys {
			if bytes.Equal(key, current) {
				lock.IterWaiters(func(pblock.WaitTxn) bool { found = true; return false })
				break
			}
		}
		return !found
	})
	return found
}
