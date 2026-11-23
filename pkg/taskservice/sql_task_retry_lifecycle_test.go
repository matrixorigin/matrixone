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
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type lostRetryCommitStore struct {
	TaskStorage
	base      *memTaskStorage
	committed bool
	ackErr    error
	injected  atomic.Bool
}

type repeatedlyRolledBackRetryStore struct {
	TaskStorage
	attempts int
	cancel   context.CancelFunc
}

func (s *repeatedlyRolledBackRetryStore) TransitionSQLTaskRun(context.Context, SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	s.attempts++
	if s.attempts == 3 {
		s.cancel()
	}
	return SQLTaskRetryTransition{CommitAttempted: true}, errors.New("injected lost COMMIT acknowledgement")
}

func (*repeatedlyRolledBackRetryStore) ReconcileSQLTaskRetry(context.Context, SQLTaskRetryRequest, uint64) (SQLTaskRetryResolution, error) {
	return SQLTaskRetryRolledBack, nil
}

type blockedRetryReconcileStore struct {
	*lostRetryCommitStore
	entered chan struct{}
}

type forcedRetryReconcileStore struct {
	*lostRetryCommitStore
}

func (s *forcedRetryReconcileStore) ReconcileSQLTaskRetry(ctx context.Context, request SQLTaskRetryRequest, nextID uint64) (SQLTaskRetryResolution, error) {
	changed, err := s.base.ForceRecoverSQLTaskRun(ctx, request.Previous.AccountID, request.Previous.TaskName, nextID, "admin")
	if err != nil {
		return 0, err
	}
	if !changed {
		return 0, errors.New("test could not force the tentative successor")
	}
	return s.base.ReconcileSQLTaskRetry(ctx, request, nextID)
}

func (s *blockedRetryReconcileStore) ReconcileSQLTaskRetry(ctx context.Context, _ SQLTaskRetryRequest, _ uint64) (SQLTaskRetryResolution, error) {
	close(s.entered)
	<-ctx.Done()
	return 0, ctx.Err()
}

type mutatingRetryStore struct {
	TaskStorage
	seen SQLTaskClaim
}

type transientRetryStore struct {
	TaskStorage
	attempts atomic.Int32
}

type blockingRetryTransitionStore struct {
	TaskStorage
	entered chan context.Context
}

func (s *blockingRetryTransitionStore) TransitionSQLTaskRun(ctx context.Context, _ SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	s.entered <- ctx
	<-ctx.Done()
	return SQLTaskRetryTransition{}, ctx.Err()
}

func (s *transientRetryStore) TransitionSQLTaskRun(ctx context.Context, request SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	if s.attempts.Add(1) == 1 {
		return SQLTaskRetryTransition{}, errors.New("injected pre-commit metadata failure")
	}
	return s.TaskStorage.TransitionSQLTaskRun(ctx, request)
}

func TestSQLTaskRetrySurvivesAdmissionContentionAndTransientFailure(t *testing.T) {
	for _, trigger := range []string{SQLTaskTriggerManual, SQLTaskTriggerScheduled} {
		t.Run(trigger, func(t *testing.T) {
			base := NewMemTaskStorage().(*memTaskStorage)
			store := &transientRetryStore{TaskStorage: base}
			service := NewTaskService(runtime.DefaultRuntime(), store)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
				definition.TriggerCount, definition.RetryLimit = 1, 1
				definition.GateCondition, definition.SQLBody = "", "insert into target values (1)"
			})
			spec := newSQLTaskContextForTest(definition, trigger)
			spec.TriggerCount = 1
			release := func() {}
			t.Cleanup(func() { release() })
			calls := 0
			fake := &fakeInternalExecutor{execHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
				calls++
				if calls == 1 {
					var err error
					_, release, err = BeginSQLTaskManualAdmission(ctx, service, definition.AccountID, definition.TaskName)
					require.NoError(t, err)
					return errors.New("first business attempt failed")
				}
				competitor := newTestSQLTaskRun(definition.TaskID, definition.TaskName, SQLTaskStatusRunning)
				_, err := base.AcquireSQLTaskRun(ctx, definition, competitor, testSQLTaskAdmission(nil, 0))
				require.ErrorIs(t, err, ErrSQLTaskOverlap)
				release()
				return nil
			}}
			executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
			var err error
			if trigger == SQLTaskTriggerScheduled {
				firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
				firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn1", 1
				firing.ParentTaskID = fmt.Sprintf("sql-task:%d", definition.TaskID)
				mustAddTestAsyncTask(t, base, 1, firing)
				firing = mustGetTestAsyncTask(t, base, 1)[0]
				err = executor.TaskExecutor()(context.Background(), &firing)
			} else {
				err = executor.ExecuteContext(context.Background(), spec, true)
			}
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.Equal(t, int32(2), store.attempts.Load())
			runs := mustGetTestSQLTaskRun(t, base, 2, WithTaskIDCond(EQ, definition.TaskID))
			require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
			require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
		})
	}
}

func (s *mutatingRetryStore) TransitionSQLTaskRun(_ context.Context, request SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	if request.Claim != nil {
		s.seen = *request.Claim
		request.Claim.ID++
	}
	return SQLTaskRetryTransition{}, nil
}

type blockingRetryArgument struct {
	entered chan struct{}
	release chan struct{}
}

func (a blockingRetryArgument) Match(value driver.Value) bool {
	close(a.entered)
	<-a.release
	return value == int64(7)
}

func TestSQLTaskRetrySnapshotsClaimAtStorageBoundaries(t *testing.T) {
	t.Run("decorator cannot change caller identity", func(t *testing.T) {
		claim := SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}
		store := &mutatingRetryStore{}
		_, err := transitionSQLTaskRun(context.Background(), store, SQLTaskRetryRequest{Claim: &claim})
		require.NoError(t, err)
		require.Equal(t, SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}, store.seen)
		require.Equal(t, store.seen, claim)
	})

	t.Run("transaction cannot observe later decorator changes", func(t *testing.T) {
		store, mock := newMockStorage(t)
		t.Cleanup(func() { _ = store.Close() })
		old := newTestSQLTaskRun(7, "owned", SQLTaskStatusFailed)
		old.RunID, old.FinishedAt = 42, time.Now().Truncate(time.Second)
		next := newTestSQLTaskRun(7, "owned", SQLTaskStatusRunning)
		next.AttemptNumber = 2
		claim := &SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}
		barrier := blockingRetryArgument{entered: make(chan struct{}), release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(barrier.release) })
		t.Cleanup(release)
		mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(lockOwnedSQLTaskAdmission).WithArgs(barrier, old.AccountID, old.TaskName).
			WillReturnRows(sqlmock.NewRows([]string{"id", "account", "name", "enabled", "generation", "count"}).AddRow(7, 1, "owned", 1, 1, 2))
		mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(uint64(9)).
			WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(int32(task.TaskStatus_Running), "cn1", 4, old.RunID))
		mock.ExpectExec(completeSQLTaskRun + " and task_id=? and account_id=?").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(insertSQLTaskRun).WillReturnResult(sqlmock.NewResult(43, 1))
		mock.ExpectExec("update sys_async_task set sql_task_run_id=? where task_id=? and sql_task_run_id=? and task_status=? and task_runner=? and task_epoch=?").
			WithArgs(uint64(43), uint64(9), old.RunID, task.TaskStatus_Running, "cn1", uint32(4)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		result := make(chan error, 1)
		t.Cleanup(func() {
			release()
			cancel()
			<-done
		})
		go func() {
			defer close(done)
			outcome, err := store.TransitionSQLTaskRun(ctx, SQLTaskRetryRequest{Previous: old, Next: next, Claim: claim})
			if err == nil && (outcome.RunID != 43 || !outcome.Continued) {
				err = fmt.Errorf("unexpected transition result: %+v", outcome)
			}
			result <- err
		}()
		select {
		case <-barrier.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("retry transaction did not start")
		}
		*claim = SQLTaskClaim{ID: 99, Runner: "other", Epoch: 9}
		release()
		require.NoError(t, <-result)
		require.NoError(t, store.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestSQLTaskMySQLRetryTransition(t *testing.T) {
	for _, loseCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_commit_response=%t", loseCommit), func(t *testing.T) {
			store, mock := newMockStorage(t)
			old := newTestSQLTaskRun(7, "owned", SQLTaskStatusFailed)
			old.RunID, old.FinishedAt = 42, time.Now().Truncate(time.Second)
			next := newTestSQLTaskRun(7, "owned", SQLTaskStatusRunning)
			next.AttemptNumber = 2
			claim := &SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}
			retry := SQLTaskRetryRequest{Previous: old, Next: next, Claim: claim}
			mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery(lockOwnedSQLTaskAdmission).WithArgs(old.TaskID, old.AccountID, old.TaskName).
				WillReturnRows(sqlmock.NewRows([]string{"id", "account", "name", "enabled", "generation", "count"}).AddRow(7, 1, "owned", 1, 1, 2))
			mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(claim.ID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(int32(task.TaskStatus_Running), claim.Runner, claim.Epoch, old.RunID))
			mock.ExpectExec(completeSQLTaskRun + " and task_id=? and account_id=?").WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(insertSQLTaskRun).WillReturnResult(sqlmock.NewResult(43, 1))
			mock.ExpectExec("update sys_async_task set sql_task_run_id=? where task_id=? and sql_task_run_id=? and task_status=? and task_runner=? and task_epoch=?").
				WithArgs(uint64(43), claim.ID, old.RunID, task.TaskStatus_Running, claim.Runner, claim.Epoch).
				WillReturnResult(sqlmock.NewResult(0, 1))
			problem := errors.New("commit acknowledgement lost")
			if loseCommit {
				mock.ExpectExec("COMMIT").WillReturnError(problem)
				mock.ExpectClose()
			} else {
				mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			outcome, err := store.TransitionSQLTaskRun(context.Background(), retry)
			require.Equal(t, uint64(43), outcome.RunID)
			require.True(t, outcome.Continued)
			require.True(t, outcome.CommitAttempted)
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

func TestSQLTaskMySQLRetryReconciliation(t *testing.T) {
	for _, scenario := range []string{"committed", "rolled_back", "lost_claim", "no_successor"} {
		t.Run(scenario, func(t *testing.T) {
			store, mock := newMockStorage(t)
			t.Cleanup(func() { _ = store.Close() })
			previous := newTestSQLTaskRun(7, "owned", SQLTaskStatusFailed)
			previous.RunID, previous.FinishedAt, previous.TriggerType = 42, time.Now().Truncate(time.Second), SQLTaskTriggerScheduled
			next := newTestSQLTaskRun(7, "owned", SQLTaskStatusRunning)
			next.RunID, next.AttemptNumber, next.TriggerType = 43, 2, SQLTaskTriggerScheduled
			claim := &SQLTaskClaim{ID: 9, Runner: "cn1", Epoch: 4}
			request := SQLTaskRetryRequest{Previous: previous, Next: next, Claim: claim}
			actualPrevious := previous
			nextID, pointer, runner, status := uint64(43), uint64(43), "cn1", task.TaskStatus_Running
			if scenario == "rolled_back" {
				actualPrevious.Status, actualPrevious.FinishedAt, pointer = SQLTaskStatusRunning, time.Time{}, previous.RunID
			}
			if scenario == "lost_claim" {
				runner = "other"
			}
			if scenario == "no_successor" {
				nextID, pointer, status = 0, previous.RunID, task.TaskStatus_Completed
			}
			rows := func(run SQLTaskRun) *sqlmock.Rows {
				return sqlmock.NewRows(sqlTaskRunRows).AddRow(run.RunID, run.TaskID, run.TaskName, run.AccountID,
					nullTime(run.ScheduledAt), nullTime(run.StartedAt), nullTime(run.FinishedAt), run.DurationSeconds,
					run.Status, run.TriggerType, run.AttemptNumber, run.RowsAffected, run.ErrorCode,
					nullString(run.ErrorMessage), sqlTaskEnabledValue(run.GateResult), run.RunnerCN)
			}
			mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectQuery("select task_id from sql_task where task_id=? and account_id=? and task_name=? for update").
				WithArgs(previous.TaskID, previous.AccountID, previous.TaskName).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow(previous.TaskID))
			mock.ExpectQuery(lockSQLTaskReceiptAsync).WithArgs(claim.ID).
				WillReturnRows(sqlmock.NewRows([]string{"status", "runner", "epoch", "run"}).AddRow(status, runner, claim.Epoch, pointer))
			mock.ExpectQuery(selectSQLTaskRun + " AND run_id=? for update").WithArgs(previous.RunID).WillReturnRows(rows(actualPrevious))
			if nextID != 0 {
				nextRows := sqlmock.NewRows(sqlTaskRunRows)
				if scenario != "rolled_back" {
					nextRows = rows(next)
				}
				mock.ExpectQuery(selectSQLTaskRun + " AND run_id=? for update").WithArgs(nextID).WillReturnRows(nextRows)
			}
			if scenario == "lost_claim" {
				mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
			} else {
				mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			resolution, err := store.ReconcileSQLTaskRetry(context.Background(), request, nextID)
			switch scenario {
			case "committed":
				require.NoError(t, err)
				require.Equal(t, SQLTaskRetryAdmitted, resolution)
			case "rolled_back":
				require.NoError(t, err)
				require.Equal(t, SQLTaskRetryRolledBack, resolution)
			case "no_successor":
				require.NoError(t, err)
				require.Equal(t, SQLTaskRetryStopped, resolution)
			case "lost_claim":
				require.ErrorIs(t, err, errSQLTaskReceiptConflict)
			}
			mock.ExpectClose()
			require.NoError(t, store.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func (s *lostRetryCommitStore) TransitionSQLTaskRun(ctx context.Context, retry SQLTaskRetryRequest) (SQLTaskRetryTransition, error) {
	if !s.injected.CompareAndSwap(false, true) {
		return s.base.TransitionSQLTaskRun(ctx, retry)
	}
	var outcome SQLTaskRetryTransition
	if s.committed {
		var err error
		outcome, err = s.base.TransitionSQLTaskRun(ctx, retry)
		if err != nil {
			return outcome, err
		}
	} else {
		s.base.Lock()
		outcome = SQLTaskRetryTransition{RunID: s.base.nextIDLocked(), Continued: true, CommitAttempted: true}
		s.base.Unlock()
	}
	if s.ackErr != nil {
		return outcome, s.ackErr
	}
	return outcome, errors.New("injected lost COMMIT acknowledgement")
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

func TestSQLTaskRetryReconcilesUncertainCommit(t *testing.T) {
	for _, trigger := range []string{SQLTaskTriggerManual, SQLTaskTriggerScheduled} {
		for _, committed := range []bool{false, true} {
			for _, ackErr := range []error{nil, errSQLTaskReceiptConflict} {
				t.Run(fmt.Sprintf("%s/committed=%t/terminal_error=%t", trigger, committed, ackErr != nil), func(t *testing.T) {
					ctx := context.Background()
					base := NewMemTaskStorage().(*memTaskStorage)
					store := &lostRetryCommitStore{TaskStorage: base, base: base, committed: committed, ackErr: ackErr}
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
					require.NoError(t, err)
					require.Len(t, fake.execSQLs, 2)
					require.Eventually(t, func() bool {
						service.sqlRecovery.mu.Lock()
						defer service.sqlRecovery.mu.Unlock()
						return service.sqlRecovery.used[sqlTaskManualReceipt] == 0 && service.sqlRecovery.used[sqlTaskScheduledReceipt] == 0
					}, 2*time.Second, time.Millisecond)
					runs := mustGetTestSQLTaskRun(t, store, 2, WithTaskIDCond(EQ, definition.TaskID))
					require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
					require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
					if trigger == SQLTaskTriggerScheduled {
						async := mustGetTestAsyncTask(t, store, 1)[0]
						require.Equal(t, task.TaskStatus_Completed, async.Status)
						require.Equal(t, task.ResultCode_Success, async.ExecuteResult.Code)
					}
				})
			}
		}
	}
}

func TestSQLTaskRetryBacksOffAfterUncertainRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := &repeatedlyRolledBackRetryStore{cancel: cancel}
	start := time.Now()
	_, err := retrySQLTaskTransition(ctx, store, SQLTaskRetryRequest{})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 3, store.attempts)
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond, "uncertain rollbacks must not spin")
}

func TestSQLTaskRetryStopsWhenReconciliationLosesOwner(t *testing.T) {
	for _, action := range []string{"cancel", "shutdown"} {
		t.Run(action, func(t *testing.T) {
			abandonedBefore := testutil.ToFloat64(v2.SQLTaskReceiptAbandonedCounter)
			base := NewMemTaskStorage().(*memTaskStorage)
			store := &blockedRetryReconcileStore{
				lostRetryCommitStore: &lostRetryCommitStore{TaskStorage: base, base: base, committed: true},
				entered:              make(chan struct{}),
			}
			service := NewTaskService(runtime.DefaultRuntime(), store)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
				definition.RetryLimit, definition.GateCondition, definition.SQLBody = 1, "", "insert into target values (1)"
			})
			problem := errors.New("first business attempt failed")
			fake := &fakeInternalExecutor{execErrs: []error{problem, nil}}
			executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			finished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("executor did not stop")
				}
			})
			go func() {
				defer close(finished)
				done <- executor.ExecuteContext(ctx, newSQLTaskContextForTest(definition, SQLTaskTriggerManual), true)
			}()
			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("retry reconciliation did not start")
			}
			if action == "cancel" {
				cancel()
			} else {
				require.NoError(t, service.Close())
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, problem)
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorContains(t, err, "retry was not executed")
			case <-time.After(5 * time.Second):
				t.Fatal("retry did not stop after losing its owner")
			}
			require.Len(t, fake.execSQLs, 1, "an unconfirmed retry must not execute SQL")
			if action == "shutdown" {
				require.Equal(t, abandonedBefore+1, testutil.ToFloat64(v2.SQLTaskReceiptAbandonedCounter),
					"a receipt rejected after shutdown must remain observable")
			} else {
				require.Eventually(t, func() bool {
					runs, err := base.QuerySQLTaskRun(context.Background(), WithTaskIDCond(EQ, definition.TaskID))
					return err == nil && len(runs) == 2 && runs[0].Status == SQLTaskStatusFailed && runs[1].Status == SQLTaskStatusFailed
				}, 5*time.Second, 10*time.Millisecond, "the live service must reconcile metadata without running SQL")
			}
		})
	}
}

func TestScheduledSQLTaskRetryTransitionHonorsCancellation(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &blockingRetryTransitionStore{TaskStorage: base, entered: make(chan context.Context, 1)}
	service := NewTaskService(runtime.DefaultRuntime(), store)
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
	problem := errors.New("first business attempt failed")
	fake := &fakeInternalExecutor{execErrs: []error{problem, nil}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
	type retryContextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), retryContextKey{}, "trace"), time.Minute)
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("scheduled executor did not stop")
		}
	})
	go func() {
		defer close(finished)
		done <- executor.TaskExecutor()(ctx, &firing)
	}()
	select {
	case transitionCtx := <-store.entered:
		outerDeadline, _ := ctx.Deadline()
		transitionDeadline, ok := transitionCtx.Deadline()
		require.True(t, ok)
		require.False(t, transitionDeadline.After(outerDeadline))
		require.Equal(t, "trace", transitionCtx.Value(retryContextKey{}))
	case <-time.After(5 * time.Second):
		t.Fatal("retry transition did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, problem)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "retry was not executed")
	case <-time.After(5 * time.Second):
		t.Fatal("retry transition ignored cancellation")
	}
	require.Len(t, fake.execSQLs, 1)
	async := mustGetTestAsyncTask(t, base, 1)[0]
	require.Equal(t, task.TaskStatus_Completed, async.Status)
	require.Equal(t, task.ResultCode_Failed, async.ExecuteResult.Code)
}

func TestSQLTaskRetryDoesNotExecuteForcedSuccessor(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &forcedRetryReconcileStore{lostRetryCommitStore: &lostRetryCommitStore{TaskStorage: base, base: base, committed: true}}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
		definition.RetryLimit, definition.GateCondition, definition.SQLBody = 1, "", "insert into target values (1)"
	})
	problem := errors.New("first business attempt failed")
	fake := &fakeInternalExecutor{execErrs: []error{problem, nil}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(definition, SQLTaskTriggerManual), true)
	require.ErrorIs(t, err, problem)
	require.ErrorContains(t, err, "retry was not executed")
	require.Len(t, fake.execSQLs, 1)
	runs := mustGetTestSQLTaskRun(t, base, 2, WithTaskIDCond(EQ, definition.TaskID))
	require.Equal(t, SQLTaskStatusUnknown, runs[0].Status)
	require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
}

func TestSQLTaskRetryReconcilesUncertainStopWithoutSuccessor(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &lostRetryCommitStore{TaskStorage: base, base: base, committed: true}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := mustAddSQLTaskForExecutorTest(t, base, func(definition *SQLTask) {
		definition.RetryLimit, definition.GateCondition, definition.SQLBody = 1, "", "insert into target values (1)"
	})
	problem := errors.New("first business attempt failed")
	fake := &fakeInternalExecutor{execHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
		_, err := base.DeleteSQLTask(ctx, WithTaskIDCond(EQ, definition.TaskID))
		require.NoError(t, err)
		return problem
	}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(definition, SQLTaskTriggerManual), true)
	require.ErrorIs(t, err, problem)
	require.ErrorContains(t, err, "retry was not executed")
	require.Len(t, fake.execSQLs, 1)
	runs := mustGetTestSQLTaskRun(t, base, 1, WithTaskIDCond(EQ, definition.TaskID))
	require.Equal(t, SQLTaskStatusFailed, runs[0].Status)
}
