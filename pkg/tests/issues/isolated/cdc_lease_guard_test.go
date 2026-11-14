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
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestOwnedCDCTaskLeaseGuardsCatalog(t *testing.T) {
	path, err := os.MkdirTemp("", "owned-cdc-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(path)) })
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(2),
		embed.WithPreStart(func(s embed.ServiceOperator) {
			if s.ServiceType() == metadata.ServiceType_CN {
				s.Adjust(func(cfg *embed.ServiceConfig) {
					cfg.CN.TaskRunner.FetchInterval.Duration = time.Hour
					cfg.CN.TaskRunner.HeartbeatInterval.Duration = time.Hour
				})
			}
		}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	first, err := cluster.GetCNService(0)
	require.NoError(t, err)
	second, err := cluster.GetCNService(1)
	require.NoError(t, err)
	var ts taskservice.TaskService
	require.Eventually(t, func() bool { var ok bool; ts, ok = first.RawService().(cnservice.Service).GetTaskService(); return ok }, 90*time.Second, 100*time.Millisecond)
	// Keep the scenario timeout independent of waiting for cluster admission.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", first.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	const taskID = "00000000-0000-0000-0000-000000000777"
	result, err := first.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx, "insert into mo_catalog.mo_cdc_task(account_id,task_id,task_name,source_uri,sink_uri,tables,state,err_msg) values(0,'"+taskID+"','owned-cdc-probe','','','{}','restarting','')", executor.Options{}.WithAccountID(0))
	require.NoError(t, err)
	result.Close()
	details := &task.Details{AccountID: 0, Account: "sys", Username: "dump", Details: &task.Details_CreateCdc{CreateCdc: &task.CreateCdcDetails{TaskId: taskID, TaskName: "owned-cdc-probe", Accounts: []*task.Account{{Id: 0}}}}}
	require.NoError(t, ts.CreateDaemonTask(ctx, task.TaskMetadata{ID: taskID, Executor: task.TaskCode_InitCdc}, details))
	rows, err := ts.QueryDaemonTask(ctx, taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	rows[0].TaskStatus = task.TaskStatus_Running
	rows[0].TaskRunner = first.ServiceID()
	rows[0].LastHeartbeat = time.Now()
	n, err := ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	owner := taskservice.CDCTaskOwner{DaemonID: rows[0].ID, Runner: first.ServiceID(), Status: task.TaskStatus_Running, Key: taskservice.CDCTaskKey{AccountId: 0, TaskId: taskID}}
	setState := func(state string) func(context.Context, taskservice.SqlExecutor) error {
		return func(c context.Context, tx taskservice.SqlExecutor) error {
			_, e := tx.ExecContext(c, "update mo_catalog.mo_cdc_task set state=? where account_id=0 and task_id=?", state, taskID)
			return e
		}
	}
	readState := func() string {
		var state string
		require.NoError(t, db.QueryRowContext(ctx, "select state from mo_catalog.mo_cdc_task where account_id=0 and task_id=?", taskID).Scan(&state))
		return state
	}
	require.NoError(t, ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, setState("running")), "matched owner no-op task status must commit")
	require.Equal(t, "running", readState())
	wrong := owner
	wrong.Runner = second.ServiceID()
	require.Error(t, ts.UpdateOwnedCDCTask(ctx, wrong, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, setState("failed")))
	require.Equal(t, "running", readState(), "lost owner must roll back the catalog")
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	done := make(chan error, 1)
	go func() {
		done <- ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, func(c context.Context, tx taskservice.SqlExecutor) error {
			if e := setState("restarting")(c, tx); e != nil {
				return e
			}
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("catalog mutation did not reach guard")
	}
	takeover := rows[0]
	takeover.TaskRunner = second.ServiceID()
	takeover.LastHeartbeat = time.Now()
	n, err = ts.UpdateDaemonTask(ctx, []task.DaemonTask{takeover})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	releaseOnce.Do(func() { close(release) })
	select {
	case err = <-done:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("guard did not finish")
	}
	require.Equal(t, "running", readState(), "superseded owner's catalog write must roll back")
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, second.ServiceID(), rows[0].TaskRunner)
	owner.Runner = second.ServiceID()
	enteredControl, releaseControl := make(chan struct{}), make(chan struct{})
	var releaseControlOnce sync.Once
	t.Cleanup(func() { releaseControlOnce.Do(func() { close(releaseControl) }) })
	done = make(chan error, 1)
	go func() {
		done <- ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, func(c context.Context, tx taskservice.SqlExecutor) error {
			if e := setState("failed")(c, tx); e != nil {
				return e
			}
			close(enteredControl)
			<-releaseControl
			return nil
		})
	}()
	select {
	case <-enteredControl:
	case <-ctx.Done():
		t.Fatal("failure publication did not reach guard")
	}
	n, err = ts.UpdateDaemonTaskStatus(ctx, owner.DaemonID, task.TaskStatus_PauseRequested,
		time.Now(), time.Time{}, taskservice.WithTaskStatusCond(task.TaskStatus_Running), taskservice.WithTaskRunnerCond(taskservice.EQ, owner.Runner))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	releaseControlOnce.Do(func() { close(releaseControl) })
	select {
	case err = <-done:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("failure guard did not finish")
	}
	require.Equal(t, "running", readState(), "a newer pause must roll back the old failure publication")
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_PauseRequested, rows[0].TaskStatus)
	rows[0].TaskStatus = task.TaskStatus_Running
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Release updates the catalog and clears the lease together; completion
	// later records the new run and heartbeat with the Running transition.
	require.NoError(t, ts.UpdateOwnedCDCTask(ctx, owner,
		taskservice.CDCTaskTransition{Status: task.TaskStatus_RestartRequested, ReleaseLease: true}, setState("restarting")))
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_RestartRequested, rows[0].TaskStatus)
	require.Empty(t, rows[0].TaskRunner)
	require.True(t, rows[0].LastHeartbeat.IsZero())
	require.Equal(t, "restarting", readState())

	rows[0].TaskRunner = second.ServiceID()
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	owner.Status = task.TaskStatus_RestartRequested
	started := time.Now().Truncate(time.Second)
	require.NoError(t, ts.UpdateOwnedCDCTask(ctx, owner,
		taskservice.CDCTaskTransition{Status: task.TaskStatus_Running, StartedAt: started}, setState("running")))
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_Running, rows[0].TaskStatus)
	require.Equal(t, started.Unix(), rows[0].LastRun.Unix())
	require.Equal(t, started.Unix(), rows[0].LastHeartbeat.Unix())
	require.Equal(t, "running", readState())

	owner.Status = task.TaskStatus_Running
	const failure = "permanent table error"
	require.NoError(t, ts.UpdateOwnedCDCTask(ctx, owner,
		taskservice.CDCTaskTransition{Status: task.TaskStatus_Error}, func(c context.Context, tx taskservice.SqlExecutor) error {
			_, e := tx.ExecContext(c, "update mo_catalog.mo_cdc_task set state='failed', err_msg=? where account_id=0 and task_id=? and state='running'", failure, taskID)
			return e
		}))
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_Error, rows[0].TaskStatus)
	require.Equal(t, "failed", readState())
	require.Error(t, ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, setState("running")))
	require.Equal(t, "failed", readState(), "an old running owner must not erase a terminal failure")
	var diagnostic string
	require.NoError(t, db.QueryRowContext(ctx, "select err_msg from mo_catalog.mo_cdc_task where account_id=0 and task_id=?", taskID).Scan(&diagnostic))
	require.Equal(t, failure, diagnostic)

	_, err = ts.UpdateCDCTask(ctx, task.TaskStatus_RestartRequested,
		func(c context.Context, _ task.TaskStatus, keys map[taskservice.CDCTaskKey]struct{}, tx taskservice.SqlExecutor) (int, error) {
			keys[owner.Key] = struct{}{}
			result, e := tx.ExecContext(c, "update mo_catalog.mo_cdc_task set state='restarting' where account_id=0 and task_id=?", taskID)
			if e != nil {
				return 0, e
			}
			count, e := result.RowsAffected()
			return int(count), e
		}, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_RestartRequested, rows[0].TaskStatus)
	require.Equal(t, "restarting", readState())
	require.NoError(t, db.QueryRowContext(ctx, "select err_msg from mo_catalog.mo_cdc_task where account_id=0 and task_id=?", taskID).Scan(&diagnostic))
	require.Equal(t, failure, diagnostic, "restart admission must preserve the cause until startup succeeds")

	// A replacement must not claim a failed restart without atomically
	// preserving the restart marker, even if it crashes before Start.
	setCatalogState := func(state string) {
		res, e := first.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
			fmt.Sprintf("update mo_catalog.mo_cdc_task set state='%s' where account_id=0 and task_id='%s'", state, taskID), executor.Options{}.WithAccountID(0))
		require.NoError(t, e)
		res.Close()
	}
	rows[0].TaskStatus = task.TaskStatus_RestartRequested
	rows[0].TaskRunner = first.ServiceID()
	rows[0].LastHeartbeat = time.Now().Add(-time.Minute)
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	setCatalogState("failed")
	require.Equal(t, "failed", readState())
	claim := rows[0]
	claim.TaskStatus = task.TaskStatus_Running
	claim.TaskRunner = second.ServiceID()
	claim.LastHeartbeat = time.Now()
	claim.LastRun = claim.LastHeartbeat
	claim.UpdateAt = claim.LastHeartbeat
	staleBefore := time.Now().Add(-10 * time.Second)
	claimed, err := ts.GetStorage().ClaimCDCRestart(ctx, claim, "another-cn", staleBefore)
	require.NoError(t, err)
	require.False(t, claimed, "a superseded claim must fail its owner guard")
	require.Equal(t, "failed", readState(), "a failed guard must roll back catalog normalization")
	rows[0].LastHeartbeat = time.Now()
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	claimed, err = ts.GetStorage().ClaimCDCRestart(ctx, claim, first.ServiceID(), staleBefore)
	require.NoError(t, err)
	require.False(t, claimed, "a fresh heartbeat must prevent takeover")
	require.Equal(t, "failed", readState())
	rows[0].LastHeartbeat = time.Now().Add(-time.Minute)
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	setCatalogState("paused")
	claimed, err = ts.GetStorage().ClaimCDCRestart(ctx, claim, first.ServiceID(), staleBefore)
	require.NoError(t, err)
	require.False(t, claimed, "a paused catalog must not be reopened by an old restart")
	require.Equal(t, "paused", readState())
	setCatalogState("failed")
	claimed, err = ts.GetStorage().ClaimCDCRestart(ctx, claim, first.ServiceID(), staleBefore)
	require.NoError(t, err)
	require.True(t, claimed)
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_Running, rows[0].TaskStatus)
	require.Equal(t, second.ServiceID(), rows[0].TaskRunner)
	require.Equal(t, "restarting", readState())
	require.NoError(t, db.QueryRowContext(ctx, "select err_msg from mo_catalog.mo_cdc_task where account_id=0 and task_id=?", taskID).Scan(&diagnostic))
	require.Equal(t, failure, diagnostic)
	for _, nullRunner := range []bool{false, true} {
		rows[0].TaskStatus = task.TaskStatus_RestartRequested
		rows[0].TaskRunner = ""
		rows[0].LastHeartbeat = time.Time{}
		n, err = ts.UpdateDaemonTask(ctx, rows)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		if nullRunner {
			res, e := first.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
				fmt.Sprintf("update mo_task.sys_daemon_task set task_runner=null where task_id=%d", rows[0].ID), executor.Options{}.WithAccountID(0))
			require.NoError(t, e)
			res.Close()
		}
		setCatalogState("failed")
		claim = rows[0]
		claim.TaskStatus = task.TaskStatus_Running
		claim.TaskRunner = second.ServiceID()
		claim.LastHeartbeat = time.Now()
		claim.LastRun, claim.UpdateAt = claim.LastHeartbeat, claim.LastHeartbeat
		claimed, err = ts.GetStorage().ClaimCDCRestart(ctx, claim, "", staleBefore)
		require.NoError(t, err)
		require.True(t, claimed, "unassigned restart with nullRunner=%t", nullRunner)
		require.Equal(t, "restarting", readState())
		rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
		require.NoError(t, err)
		require.Equal(t, second.ServiceID(), rows[0].TaskRunner)
	}
}
