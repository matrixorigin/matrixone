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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
	withOptimisticTakeover := func(update func()) {
		t.Helper()
		type runtimeSettings struct {
			runtime               moruntime.Runtime
			mode, isolation       any
			hadMode, hadIsolation bool
		}
		settings := make([]runtimeSettings, 0, 2)
		for _, id := range []string{first.ServiceID(), second.ServiceID()} {
			rt := moruntime.ServiceRuntime(id)
			mode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
			isolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
			settings = append(settings, runtimeSettings{rt, mode, isolation, hadMode, hadIsolation})
			rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
			rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
		}
		defer func() {
			for _, setting := range settings {
				if !setting.hadMode {
					setting.mode = pbtxn.TxnMode_Pessimistic
				}
				if !setting.hadIsolation {
					setting.isolation = pbtxn.TxnIsolation_RC
				}
				setting.runtime.SetGlobalVariables(moruntime.TxnMode, setting.mode)
				setting.runtime.SetGlobalVariables(moruntime.TxnIsolation, setting.isolation)
			}
		}()
		update()
	}
	takeoverDaemon := func(row task.DaemonTask, previousRunner, nextRunner string, status task.TaskStatus) {
		t.Helper()
		row.TaskRunner = nextRunner
		row.LastHeartbeat = time.Now()
		withOptimisticTakeover(func() {
			updated, updateErr := ts.UpdateDaemonTask(ctx, []task.DaemonTask{row},
				taskservice.WithTaskRunnerCond(taskservice.EQ, previousRunner),
				taskservice.WithTaskStatusCond(status))
			require.NoError(t, updateErr)
			require.Equal(t, 1, updated)
		})
	}
	waitOwner := func(expected string) {
		t.Helper()
		var visible string
		var readErr error
		require.Eventually(t, func() bool {
			readErr = db.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", owner.DaemonID).Scan(&visible)
			return readErr != nil || visible == expected
		}, 30*time.Second, 100*time.Millisecond, "successor owner must be visible before the stale transaction commits")
		require.NoError(t, readErr)
	}
	require.NoError(t, ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, setState("running")), "matched owner no-op task status must commit")
	require.Equal(t, "running", readState())
	// A successor may use an optimistic transaction and bypass the old CN's
	// pessimistic row lock. It must still win if it commits after the guard.
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	const waiters = "owned-cdc-lease-guard-waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, taskservice.CDCLeaseGuardBeforeCommitFault, false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), taskservice.CDCLeaseGuardBeforeCommitFault)
		_, _ = fault.RemoveFaultPoint(context.Background(), waiters)
	})
	postGuardDone := make(chan error, 1)
	postGuardFinished := make(chan struct{})
	go func() {
		defer close(postGuardFinished)
		postGuardDone <- ts.UpdateOwnedCDCTask(ctx, owner, taskservice.CDCTaskTransition{Status: task.TaskStatus_Running}, setState("failed"))
	}()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), taskservice.CDCLeaseGuardBeforeCommitFault)
		select {
		case <-postGuardFinished:
		case <-time.After(15 * time.Second):
			t.Error("post-guard CDC publication goroutine did not exit")
		}
	})
	require.Eventually(t, func() bool { n, _, ok := fault.TriggerFault(waiters); return ok && n == 1 }, 30*time.Second, 100*time.Millisecond)
	takeoverDaemon(rows[0], owner.Runner, second.ServiceID(), task.TaskStatus_Running)
	var visibleOwner string
	var visibilityErr error
	require.Eventually(t, func() bool {
		visibilityErr = db.QueryRowContext(ctx, "select task_runner from mo_task.sys_daemon_task where task_id=?", owner.DaemonID).Scan(&visibleOwner)
		return visibilityErr != nil || visibleOwner == second.ServiceID()
	}, 30*time.Second, 100*time.Millisecond, "successor must be visible on the first CN before the old commit")
	require.NoError(t, visibilityErr)
	_, err = fault.RemoveFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault)
	require.NoError(t, err)
	select {
	case err = <-postGuardDone:
		var conflict *mysqlDriver.MySQLError
		require.ErrorAs(t, err, &conflict)
		require.EqualValues(t, moerr.ErrTxnWWConflict, conflict.Number)
	case <-ctx.Done():
		t.Fatal("post-guard CDC publication did not finish")
	}
	require.Equal(t, "running", readState(), "a stale owner must not publish its catalog update")
	rowsAfter, err := ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Len(t, rowsAfter, 1)
	require.Equal(t, second.ServiceID(), rowsAfter[0].TaskRunner)
	// Restore the original owner for the independent controls below.
	n, err = ts.UpdateDaemonTask(ctx, rows, taskservice.WithTaskRunnerCond(taskservice.EQ, second.ServiceID()))
	require.NoError(t, err)
	require.Equal(t, 1, n)
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
	var statusErr error
	require.Eventually(t, func() bool {
		rows, statusErr = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
		return statusErr == nil && len(rows) == 1 && rows[0].TaskStatus == task.TaskStatus_Error
	}, 30*time.Second, 100*time.Millisecond, "terminal task status must become visible after CDC publication commits")
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

	// Restart admission has the same stale-owner commit boundary: it changes
	// catalog state and the daemon lease in one fixed-snapshot transaction.
	rows[0].TaskStatus = task.TaskStatus_RestartRequested
	rows[0].TaskRunner = first.ServiceID()
	rows[0].LastHeartbeat = time.Now().Add(-time.Minute)
	n, err = ts.UpdateDaemonTask(ctx, rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	setCatalogState("failed")
	claim = rows[0]
	claim.TaskStatus = task.TaskStatus_Running
	claim.TaskRunner = second.ServiceID()
	claim.LastHeartbeat = time.Now()
	claim.LastRun, claim.UpdateAt = claim.LastHeartbeat, claim.LastHeartbeat
	claimWaiters := "owned-cdc-claim-waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, claimWaiters, ":::", "getwaiters", 0, taskservice.CDCLeaseGuardBeforeCommitFault, false))
	type claimResult struct {
		ok  bool
		err error
	}
	claimDone := make(chan claimResult, 1)
	claimFinished := make(chan struct{})
	go func() {
		defer close(claimFinished)
		ok, err := ts.GetStorage().ClaimCDCRestart(ctx, claim, first.ServiceID(), time.Now().Add(-10*time.Second))
		claimDone <- claimResult{ok, err}
	}()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), taskservice.CDCLeaseGuardBeforeCommitFault)
		_, _ = fault.RemoveFaultPoint(context.Background(), claimWaiters)
		select {
		case <-claimFinished:
		case <-time.After(15 * time.Second):
			t.Error("CDC restart claim goroutine did not exit")
		}
	})
	require.Eventually(t, func() bool { waiters, _, ok := fault.TriggerFault(claimWaiters); return ok && waiters == 1 }, 30*time.Second, 100*time.Millisecond)
	rows[0].LastHeartbeat = time.Now()
	takeoverDaemon(rows[0], first.ServiceID(), "successor-claim", task.TaskStatus_RestartRequested)
	waitOwner("successor-claim")
	_, err = fault.RemoveFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault)
	require.NoError(t, err)
	var staleClaim claimResult
	select {
	case staleClaim = <-claimDone:
	case <-ctx.Done():
		t.Fatal("stale CDC restart claim did not finish")
	}
	var claimConflict *mysqlDriver.MySQLError
	require.ErrorAs(t, staleClaim.err, &claimConflict)
	require.EqualValues(t, moerr.ErrTxnWWConflict, claimConflict.Number)
	require.False(t, staleClaim.ok)
	require.Equal(t, "failed", readState(), "a stale restart claim must not publish catalog normalization")
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_RestartRequested, rows[0].TaskStatus)
	require.Equal(t, "successor-claim", rows[0].TaskRunner)

	// User controls also update task status and CDC catalog state atomically.
	// A concurrent handoff must roll both changes back instead of restoring the
	// runner read before the handoff.
	rows[0].TaskStatus = task.TaskStatus_Running
	rows[0].TaskRunner = second.ServiceID()
	rows[0].LastHeartbeat = time.Now()
	n, err = ts.UpdateDaemonTask(ctx, rows,
		taskservice.WithTaskRunnerCond(taskservice.EQ, "successor-claim"),
		taskservice.WithTaskStatusCond(task.TaskStatus_RestartRequested))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	setCatalogState("running")
	controlWaiters := "owned-cdc-control-waiters"
	require.NoError(t, fault.AddFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, controlWaiters, ":::", "getwaiters", 0, taskservice.CDCLeaseGuardBeforeCommitFault, false))
	type controlResult struct {
		count int
		err   error
	}
	controlDone := make(chan controlResult, 1)
	controlFinished := make(chan struct{})
	go func() {
		defer close(controlFinished)
		count, err := ts.UpdateCDCTask(ctx, task.TaskStatus_RestartRequested,
			func(c context.Context, _ task.TaskStatus, keys map[taskservice.CDCTaskKey]struct{}, tx taskservice.SqlExecutor) (int, error) {
				keys[owner.Key] = struct{}{}
				result, err := tx.ExecContext(c, "update mo_catalog.mo_cdc_task set state='restarting' where account_id=0 and task_id=? and state='running'", taskID)
				if err != nil {
					return 0, err
				}
				affected, err := result.RowsAffected()
				return int(affected), err
			}, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
		controlDone <- controlResult{count, err}
	}()
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), taskservice.CDCLeaseGuardBeforeCommitFault)
		_, _ = fault.RemoveFaultPoint(context.Background(), controlWaiters)
		select {
		case <-controlFinished:
		case <-time.After(15 * time.Second):
			t.Error("CDC control update goroutine did not exit")
		}
	})
	require.Eventually(t, func() bool { waiters, _, ok := fault.TriggerFault(controlWaiters); return ok && waiters == 1 }, 30*time.Second, 100*time.Millisecond)
	takeoverDaemon(rows[0], second.ServiceID(), "successor-control", task.TaskStatus_Running)
	waitOwner("successor-control")
	_, err = fault.RemoveFaultPoint(ctx, taskservice.CDCLeaseGuardBeforeCommitFault)
	require.NoError(t, err)
	var staleControl controlResult
	select {
	case staleControl = <-controlDone:
	case <-ctx.Done():
		t.Fatal("stale CDC control update did not finish")
	}
	var controlConflict *mysqlDriver.MySQLError
	require.ErrorAs(t, staleControl.err, &controlConflict)
	require.EqualValues(t, moerr.ErrTxnWWConflict, controlConflict.Number)
	require.Equal(t, "running", readState(), "a stale task control must not publish its catalog change")
	rows, err = ts.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, owner.DaemonID))
	require.NoError(t, err)
	require.Equal(t, task.TaskStatus_Running, rows[0].TaskStatus)
	require.Equal(t, "successor-control", rows[0].TaskRunner)
	setCatalogState("failed")

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
