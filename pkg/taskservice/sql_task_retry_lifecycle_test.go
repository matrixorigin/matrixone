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

package taskservice

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type lostRetryCommitStore struct {
	TaskStorage
	base      *memTaskStorage
	committed bool
}

func TestSQLTaskMySQLRetryTransition(t *testing.T) {
	for _, loseCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_commit_response=%t", loseCommit), func(t *testing.T) {
			store, mock := newMockStorage(t)
			old := newTestSQLTaskRun(7, "owned", SQLTaskStatusFailed)
			old.RunID, old.FinishedAt = 42, time.Now().Truncate(time.Second)
			next := newTestSQLTaskRun(7, "owned", SQLTaskStatusRunning)
			next.AttemptNumber = 2
			claim := &sqlTaskAsyncClaim{id: 9, runner: "cn1", epoch: 4}
			retry := sqlTaskRetry{previous: old, next: next, claim: claim}
			mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(lockOwnedSQLTaskAdmission).WithArgs(old.TaskID, old.AccountID, old.TaskName).
				WillReturnRows(sqlmock.NewRows([]string{"id", "account", "name", "enabled", "generation", "count"}).AddRow(7, 1, "owned", 1, 1, 2))
			mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(claim.id).
				WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(int32(task.TaskStatus_Running), claim.runner, claim.epoch, old.RunID))
			mock.ExpectExec(completeSQLTaskRun + " and task_id=? and account_id=?").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(insertSQLTaskRun).WillReturnResult(sqlmock.NewResult(43, 1))
			mock.ExpectExec("update sys_async_task set sql_task_run_id=? where task_id=? and sql_task_run_id=? and task_status=? and task_runner=? and task_epoch=?").
				WithArgs(uint64(43), claim.id, old.RunID, task.TaskStatus_Running, claim.runner, claim.epoch).
				WillReturnResult(sqlmock.NewResult(0, 1))
			problem := errors.New("commit acknowledgement lost")
			if loseCommit {
				mock.ExpectExec("COMMIT").WillReturnError(problem)
				mock.ExpectClose()
			} else {
				mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			id, continued, err := store.transitionSQLTaskRun(context.Background(), retry)
			require.Equal(t, uint64(43), id)
			require.True(t, continued)
			if loseCommit {
				require.ErrorIs(t, err, problem)
			} else {
				require.NoError(t, err)
				mock.ExpectClose()
			}
			require.NoError(t, store.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func (s *lostRetryCommitStore) TransitionSQLTaskRun(ctx context.Context, retry SQLTaskRetryRequest) (uint64, bool, error) {
	var id uint64
	if s.committed {
		var err error
		id, _, err = s.base.TransitionSQLTaskRun(ctx, retry)
		if err != nil {
			return 0, false, err
		}
	} else {
		s.base.Lock()
		id = s.base.nextIDLocked()
		s.base.Unlock()
	}
	return id, true, errors.New("injected lost COMMIT acknowledgement")
}

func (s *lostRetryCommitStore) PublishSQLTaskReceipt(ctx context.Context, receipt SQLTaskReceiptRequest) (bool, error) {
	return s.base.PublishSQLTaskReceipt(ctx, receipt)
}

func TestSQLTaskRetryKeepsExecutionOwnership(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), base)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
		definition.TriggerCount = 1
		definition.GateCondition = ""
		definition.SQLBody = "insert into target values (1)"
		definition.RetryLimit = 1
	})
	spec := newSQLTaskContextForTest(definition, SQLTaskTriggerScheduled)
	spec.TriggerCount = 1
	firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
	firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
	firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
	mustAddTestAsyncTask(t, base, 1, firing)
	firing = mustGetTestAsyncTask(t, base, 1)[0]
	secondStarted, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	calls := 0
	fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
		calls++
		if calls == 1 {
			return moerr.NewInternalErrorNoCtx("first attempt failed")
		}
		close(secondStarted)
		<-release
		return nil
	}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
	done := make(chan error, 1)
	go func() { done <- executor.TaskExecutor()(ctx, &firing) }()
	select {
	case <-secondStarted:
	case err := <-done:
		t.Fatalf("scheduled firing finished before its configured retry: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("second attempt did not start")
	}
	runs := mustGetTestSQLTaskRun(t, base, 2, WithTaskIDCond(EQ, definition.TaskID))
	require.Equal(t, SQLTaskStatusRunning, runs[0].Status)
	require.Equal(t, 2, runs[0].AttemptNumber)
	require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
	require.Equal(t, definition.TaskID, runs[1].TaskID)
	require.Equal(t, definition.TaskName, runs[1].TaskName)
	require.Equal(t, SQLTaskTriggerScheduled, runs[1].TriggerType)
	require.Equal(t, time.UnixMilli(spec.ScheduledAt), runs[1].ScheduledAt)
	require.False(t, runs[1].StartedAt.IsZero())
	require.Contains(t, runs[1].ErrorMessage, "first attempt failed")
	async := mustGetTestAsyncTask(t, base, 1)[0]
	require.Equal(t, task.TaskStatus_Running, async.Status)
	base.RLock()
	claimID := base.sqlTaskClaims[async.ID]
	base.RUnlock()
	require.Equal(t, runs[0].RunID, claimID)
	competitor := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
	competitor.TriggerType = SQLTaskTriggerManual
	_, err := base.AcquireSQLTaskRun(ctx, definition, competitor, testSQLTaskAdmission(nil, 0))
	close(release)
	require.ErrorIs(t, err, ErrSQLTaskOverlap, "a retry must retain task ownership")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("scheduled retry did not finish")
	}
	require.Len(t, fake.execSQLs, 2, "a configured retry must actually execute")
	completed := mustGetTestAsyncTask(t, base, 1)[0]
	require.Equal(t, task.TaskStatus_Completed, completed.Status)
	require.Equal(t, task.ResultCode_Success, completed.ExecuteResult.Code)
}

func TestSQLTaskRetryAfterScheduleChangeOrForce(t *testing.T) {
	for _, action := range []string{"suspend", "schedule", "force"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			base := NewMemTaskStorage().(*memTaskStorage)
			service := NewTaskService(runtime.DefaultRuntime(), base)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
				definition.TriggerCount, definition.RetryLimit = 1, 1
				definition.GateCondition, definition.SQLBody = "", "insert into target values (1)"
			})
			spec := newSQLTaskContextForTest(definition, SQLTaskTriggerScheduled)
			spec.TriggerCount = 1
			firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
			firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
			firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
			mustAddTestAsyncTask(t, base, 1, firing)
			firing = mustGetTestAsyncTask(t, base, 1)[0]
			calls := 0
			firstFailure := errors.New("first attempt failed")
			fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
				calls++
				if calls != 1 {
					return nil
				}
				switch action {
				case "suspend":
					_, err := base.AlterSQLTask(ctx, definition, definition, SQLTaskSuspend)
					require.NoError(t, err)
				case "schedule":
					changed := definition
					changed.CronExpr = "0 */5 * * * *"
					_, err := base.AlterSQLTask(ctx, definition, changed, SQLTaskSetSchedule)
					require.NoError(t, err)
				case "force":
					run := mustGetTestSQLTaskRun(t, base, 1)[0]
					changed, err := ForceRecoverSQLTaskRun(ctx, base, definition.AccountID, definition.TaskName, run.RunID, "admin")
					require.NoError(t, err)
					require.True(t, changed)
				}
				return firstFailure
			}}
			executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
			err := executor.TaskExecutor()(ctx, &firing)
			if action == "force" {
				require.ErrorIs(t, err, firstFailure)
				require.Equal(t, 1, calls)
				require.Equal(t, SQLTaskStatusUnknown, mustGetTestSQLTaskRun(t, base, 1)[0].Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, calls, "an already-started firing retains its configured retry")
				require.Equal(t, SQLTaskStatusSuccess, mustGetTestSQLTaskRun(t, base, 2)[0].Status)
			}
		})
	}
}

func TestSQLTaskRetryCommitUncertainNeverRunsNextBody(t *testing.T) {
	for _, trigger := range []string{SQLTaskTriggerManual, SQLTaskTriggerScheduled} {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/committed=%t", trigger, committed), func(t *testing.T) {
				ctx := context.Background()
				base := NewMemTaskStorage().(*memTaskStorage)
				store := &lostRetryCommitStore{TaskStorage: base, base: base, committed: committed}
				service := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
					definition.TriggerCount, definition.RetryLimit = 1, 1
					definition.GateCondition, definition.SQLBody = "", "insert into target values (1)"
				})
				problem := moerr.NewInternalErrorNoCtx("first attempt failed")
				fake := &fakeInternalExecutor{execErrs: []error{problem, nil}}
				executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
				spec := newSQLTaskContextForTest(definition, trigger)
				spec.TriggerCount = 1
				var err error
				if trigger == SQLTaskTriggerScheduled {
					firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
					firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
					firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
					mustAddTestAsyncTask(t, store, 1, firing)
					firing = mustGetTestAsyncTask(t, store, 1)[0]
					err = executor.TaskExecutor()(ctx, &firing)
				} else {
					err = executor.ExecuteContext(ctx, spec, true)
				}
				require.ErrorIs(t, err, problem)
				var pending *SQLTaskMetadataPendingError
				require.ErrorAs(t, err, &pending)
				require.True(t, pending.Recovering)
				require.Len(t, fake.execSQLs, 1)
				require.Eventually(t, func() bool {
					service.sqlRecovery.mu.Lock()
					defer service.sqlRecovery.mu.Unlock()
					return service.sqlRecovery.used[sqlTaskManualReceipt] == 0 && service.sqlRecovery.used[sqlTaskScheduledReceipt] == 0
				}, 2*time.Second, time.Millisecond)
				expected := 1
				if committed {
					expected = 2
				}
				runs := mustGetTestSQLTaskRun(t, store, expected, WithTaskIDCond(EQ, definition.TaskID))
				require.Equal(t, SQLTaskStatusFailed, runs[0].Status)
				if committed {
					require.Contains(t, runs[0].ErrorMessage, "SQL did not start")
				}
				if trigger == SQLTaskTriggerScheduled {
					async := mustGetTestAsyncTask(t, store, 1)[0]
					require.Equal(t, task.TaskStatus_Completed, async.Status)
					require.Equal(t, task.ResultCode_Failed, async.ExecuteResult.Code)
				}
			})
		}
	}
}
