// Copyright 2022 Matrix Origin
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskExecutorExecuteContext(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select 1; select 2"
		task.CreatorUserID = 11
		task.CreatorRoleID = 22
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	require.Len(t, fakeIE.querySQLs, 1)
	require.Equal(t, "select (1) as gate_result", fakeIE.querySQLs[0])
	require.Len(t, fakeIE.execSQLs, 2)
	require.Equal(t, "select 1", fakeIE.execSQLs[0])
	require.Equal(t, "select 2", fakeIE.execSQLs[1])
	require.NotNil(t, fakeIE.queryOpts[0].AccountId)
	require.Equal(t, sqlTask.AccountID, *fakeIE.queryOpts[0].AccountId)
	require.NotNil(t, fakeIE.queryOpts[0].UserId)
	require.Equal(t, sqlTask.CreatorUserID, *fakeIE.queryOpts[0].UserId)
	require.NotNil(t, fakeIE.queryOpts[0].DefaultRoleId)
	require.Equal(t, sqlTask.CreatorRoleID, *fakeIE.queryOpts[0].DefaultRoleId)
	require.Nil(t, fakeIE.queryOpts[0].IsInternal)
	for _, opts := range fakeIE.execOpts {
		require.Nil(t, opts.IsInternal)
	}

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
	require.True(t, runs[0].GateResult)
}

type sqlTaskAdmissionFailureStore struct {
	TaskStorage
	err error
}

func (s *sqlTaskAdmissionFailureStore) AcquireSQLTaskRun(ctx context.Context, _ SQLTask, _ SQLTaskRun, _ *SQLTaskRunAdmission) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return 0, s.err
}

func TestSQLTaskAdmissionBackoffOnlyForStorageFailure(t *testing.T) {
	for _, name := range []string{"canceled", "holder-busy", "holder-busy-unclaimed", "storage-timeout"} {
		t.Run(name, func(t *testing.T) {
			base := NewMemTaskStorage()
			var store TaskStorage = &sqlTaskAdmissionFailureStore{TaskStorage: base, err: context.DeadlineExceeded}
			var holder *refreshableTaskStorage
			if strings.HasPrefix(name, "holder-busy") {
				holder = newRefreshableTaskStorage(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) { return "ready", nil }, NewFixedTaskStorageFactory(base)).(*refreshableTaskStorage)
				store = holder
			}
			service := NewTaskService(runtime.DefaultRuntime(), store)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			spec := &task.SQLTaskContext{TaskId: 1, TaskName: "t", TriggerType: SQLTaskTriggerScheduled, TriggerCount: 1}
			firing := task.AsyncTask{ID: 1, ParentTaskID: "sql-task:1", TaskRunner: "cn1", Metadata: BuildSQLTaskMetadata(spec)}
			if name == "holder-busy-unclaimed" {
				firing.Metadata.Context = []byte{0xff}
			}
			disposition := &sqlTaskRunnerDisposition{}
			ctx := context.WithValue(context.Background(), sqlTaskDispositionContextKey{}, disposition)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if name == "canceled" {
				cancel()
			}
			if holder != nil {
				holder.mu.Lock()
			}
			executor := NewSQLTaskExecutor(func() ie.InternalExecutor {
				t.Fatal("failed admission must not execute SQL")
				return nil
			}, service, "cn1")
			err := executor.TaskExecutor()(ctx, &firing)
			if holder != nil {
				holder.mu.Unlock()
			}
			if name == "holder-busy-unclaimed" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.True(t, disposition.deferred)
			require.Equal(t, name == "storage-timeout", disposition.failed)
		})
	}
}

func TestSQLTaskRunnerRefetchesAfterUnclaimedGuardContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerFetchInterval(5*time.Millisecond))
	t.Cleanup(func() {
		require.NoError(t, runner.Stop())
		require.NoError(t, service.Close())
	})
	definition := mustAddSQLTaskForExecutorTest(t, store, func(value *SQLTask) { value.Enabled = false })
	parent := fmt.Sprintf("sql-task:%d", definition.TaskID)
	firings := make([]task.AsyncTask, 2)
	for i := range firings {
		metadata := BuildSQLTaskMetadata(newSQLTaskContextForTest(definition, SQLTaskTriggerScheduled))
		metadata.ID = fmt.Sprintf("%s:%d", parent, i+1)
		firings[i] = task.AsyncTask{Metadata: metadata, ParentTaskID: parent, Status: task.TaskStatus_Created}
	}
	firings[0].Status, firings[0].TaskRunner, firings[0].Epoch = task.TaskStatus_Running, "cn1", 1
	mustAddTestAsyncTask(t, store, 2, firings...)

	owner := service.(*taskService)
	guard := owner.sqlRecovery.tryGuard(sqlTaskGuardKey{kind: 3, value: "unclaimed"}, 50)
	require.NotNil(t, guard)
	t.Cleanup(func() { owner.sqlRecovery.releaseGuard(guard) })
	var sqlCalls, attempts atomic.Int32
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor {
		sqlCalls.Add(1)
		return &fakeInternalExecutor{}
	}, service, "cn1").TaskExecutor()
	type attemptResult struct {
		disposition sqlTaskRunnerDisposition
		err         error
	}
	first := make(chan attemptResult, 1)
	runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, value task.Task) error {
		attempts.Add(1)
		err := executor(ctx, value)
		result := attemptResult{*ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition), err}
		select {
		case first <- result:
		default:
		}
		return err
	})
	require.NoError(t, runner.Start())
	select {
	case result := <-first:
		require.NoError(t, result.err, "nil may mean deferred rather than completed")
		require.True(t, result.disposition.handled)
		require.True(t, result.disposition.deferred)
		require.False(t, result.disposition.failed, "guard contention is not a storage failure")
	case <-ctx.Done():
		t.Fatal("runner did not reach the occupied unclaimed guard")
	}
	children := mustGetTestAsyncTask(t, store, 2, WithTaskParentTaskIDCond(EQ, parent))
	for _, child := range children {
		require.NotEqual(t, task.TaskStatus_Completed, child.Status)
	}
	mustGetTestSQLTaskRun(t, store, 0)
	require.Zero(t, sqlCalls.Load())

	owner.sqlRecovery.releaseGuard(guard)
	guard = nil
	require.Eventually(t, func() bool {
		values, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, parent))
		return err == nil && len(values) == 1 && values[0].Metadata.ID == firings[0].Metadata.ID &&
			values[0].Status == task.TaskStatus_Completed && values[0].ExecuteResult != nil &&
			values[0].ExecuteResult.Code == task.ResultCode_Success
	}, 5*time.Second, 5*time.Millisecond, "the runner must refetch and clean stale siblings after guard release")
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	mustGetTestSQLTaskRun(t, store, 0)
	require.Zero(t, sqlCalls.Load())
}

func TestSQLTaskExecutorRecordsRowsAffected(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "insert into t values (1), (2); update t set v = v + 1 where v = 1; select 1"
	})

	fakeIE := &fakeInternalExecutor{
		queryValues:      []any{true},
		execAffectedRows: []uint64{2, 1, 0},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
	require.Equal(t, int64(3), runs[0].RowsAffected)
}

func TestSQLTaskExecutorRecordsPartialRowsAffectedOnFailure(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "insert into t values (1), (2); insert into missing values (1); insert into t values (3)"
		task.RetryLimit = 0
	})

	fakeIE := &fakeInternalExecutor{
		queryValues:      []any{true},
		execAffectedRows: []uint64{2},
		execErrs:         []error{nil, moerr.NewInternalErrorNoCtx("missing table")},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.Error(t, err)

	require.Equal(t, []string{
		"insert into t values (1), (2)",
		"insert into missing values (1)",
	}, fakeIE.execSQLs)
	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusFailed, runs[0].Status)
	require.Equal(t, int64(2), runs[0].RowsAffected)
	require.Contains(t, runs[0].ErrorMessage, "missing table")
}

func TestSQLTaskExecutorPreservesQuotedStrings(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "insert into gate_sink select 'gate-ok'; insert into gate_sink values ('O''Brien'); select case when 1 = 1 then 'PASS' else 'FAIL' end"
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	require.Equal(t, []string{
		"insert into gate_sink select 'gate-ok'",
		"insert into gate_sink values ('O''Brien')",
		"select case when 1 = 1 then 'PASS' else 'FAIL' end",
	}, fakeIE.execSQLs)
}

func TestSQLTaskExecutorPreservesTimestampDiffUnits(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select timestampdiff(hour, current_timestamp(), current_timestamp())"
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	require.Equal(t, []string{
		"select timestampdiff(hour, current_timestamp(), current_timestamp())",
	}, fakeIE.execSQLs)
}

func TestSQLTaskExecutorPreservesComplexTimestampDiffUnits(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = `
insert into silver_fiix_offline_tracker
select
    work_order_id,
    hp_pump_code,
    min(offline_start) as offline_start,
    max(offline_end) as offline_end,
    timestampdiff(hour, min(offline_start), max(offline_end)) as downtime_hours
from t
group by work_order_id, hp_pump_code;

insert into gold_off_session_wo_match
with proximity_match as (
    select
        row_number() over (
            partition by s.pump, s.session_id
            order by abs(timestampdiff(minute, s.session_start, wo.dtm_date_created))
        ) as rn
    from t
    where abs(timestampdiff(hour, s.session_start, wo.dtm_date_created)) <= 4
)
select * from proximity_match`
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	executed := strings.Join(fakeIE.execSQLs, "\n")
	require.NotContains(t, executed, "timestampdiff('hour'")
	require.NotContains(t, executed, "timestampdiff('minute'")
	require.Contains(t, executed, "timestampdiff(hour, min(offline_start), max(offline_end))")
	require.Contains(t, executed, "timestampdiff(minute, s.session_start, wo.dtm_date_created)")
	require.Contains(t, executed, "timestampdiff(hour, s.session_start, wo.dtm_date_created)")
}

func TestSQLTaskExecutorSkipsWhenGateFalse(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "0"
		task.SQLBody = "select 1"
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{false},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)
	require.Empty(t, fakeIE.execSQLs)

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSkipped, runs[0].Status)
	require.False(t, runs[0].GateResult)
}

func TestSQLTaskExecutorReturnsFinishRunErrorOnGateFailure(t *testing.T) {
	baseStore := NewMemTaskStorage().(*memTaskStorage)
	store := &finishRunFailingTaskStorage{
		TaskStorage: baseStore,
	}
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, baseStore, func(task *SQLTask) {
		task.GateCondition = "select 1"
		task.SQLBody = "select 1"
		task.RetryLimit = 0
	})

	fakeIE := &fakeInternalExecutor{
		queryErrs: []error{moerr.NewInternalErrorNoCtx("gate failed")},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.ErrorContains(t, err, "gate failed")
	var pending *SQLTaskMetadataPendingError
	require.ErrorAs(t, err, &pending)
	require.True(t, pending.Recovering)

	runs := mustGetTestSQLTaskRun(t, baseStore, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusRunning, runs[0].Status)
}

func TestSQLTaskExecutorRetryOnFailure(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select 1"
		task.RetryLimit = 1
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true, true},
		execErrs:    []error{moerr.NewInternalErrorNoCtx("boom"), nil},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)

	runs := mustGetTestSQLTaskRun(t, store, 2, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
	require.Equal(t, 2, runs[0].AttemptNumber)
	require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
	require.Equal(t, 1, runs[1].AttemptNumber)
}

func TestSQLTaskExecutorTimeout(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select 1"
		task.TimeoutSeconds = 1
		task.RetryLimit = 0
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
		execHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.Error(t, err)

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusTimeout, runs[0].Status)
}

func TestSQLTaskExecutorDoesNotRetryUnknownCommit(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(error) error
	}{
		{"direct", func(err error) error { return err }},
		{"wrapped", func(err error) error { return fmt.Errorf("statement: %w", err) }},
		{"joined", func(err error) error { return errors.Join(moerr.NewInternalErrorNoCtx("cleanup"), err) }},
	} {
		for _, trigger := range []string{SQLTaskTriggerManual, SQLTaskTriggerScheduled} {
			t.Run(tc.name+"/"+trigger, func(t *testing.T) {
				ctx := context.Background()
				store := NewMemTaskStorage().(*memTaskStorage)
				service := NewTaskService(runtime.DefaultRuntime(), store)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
					task.GateCondition = ""
					task.SQLBody = "insert into target values (1)"
					task.RetryLimit = 1
					task.TriggerCount = 1
				})
				unknown := moerr.NewTxnUnknown(ctx, "txn-1")
				calls := 0
				fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
					calls++
					if calls == 1 {
						return tc.wrap(unknown)
					}
					return nil
				}}
				executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn-test")
				spec := newSQLTaskContextForTest(sqlTask, trigger)
				var err error
				if trigger == SQLTaskTriggerScheduled {
					spec.TriggerCount = sqlTask.TriggerCount
					asyncTask := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
					asyncTask.ParentTaskID = fmt.Sprintf("sql-task:%d", sqlTask.TaskID)
					asyncTask.Status, asyncTask.TaskRunner, asyncTask.Epoch = task.TaskStatus_Running, "cn-test", 1
					mustAddTestAsyncTask(t, store, 1, asyncTask)
					asyncTask = mustGetTestAsyncTask(t, store, 1)[0]
					err = executor.TaskExecutor()(ctx, &asyncTask)
				} else {
					err = executor.ExecuteContext(ctx, spec, true)
				}
				require.ErrorIs(t, err, unknown)
				var identity *moerr.Error
				require.ErrorAs(t, err, &identity)
				require.Equal(t, moerr.ErrTxnUnknown, identity.ErrorCode())
				require.Equal(t, 1, calls, "an uncertain commit must not be replayed")
				runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
				require.Equal(t, SQLTaskStatusRunning, runs[0].Status)
				require.Zero(t, runs[0].FinishedAt)
				require.Equal(t, int(moerr.ErrTxnUnknown), runs[0].ErrorCode)
				require.Contains(t, runs[0].ErrorMessage, "txn-1")
				_, acquireErr := store.AcquireSQLTaskRun(ctx, sqlTask, SQLTaskRun{TaskID: sqlTask.TaskID, TaskName: sqlTask.TaskName, AccountID: sqlTask.AccountID, Status: SQLTaskStatusRunning, TriggerType: SQLTaskTriggerManual}, testSQLTaskAdmission(nil, 0))
				require.ErrorIs(t, acquireErr, ErrSQLTaskOverlap)
			})
		}
	}
}

func TestSQLTaskScheduledRunsRecoverAfterBusinessFailure(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = ""
		task.SQLBody = "insert into target values (1)"
		task.RetryLimit = 0
		task.TriggerCount = 3
	})
	problem := moerr.NewInternalErrorNoCtx("dependency unavailable")
	failed := true
	fake := &fakeInternalExecutor{execHook: func(context.Context, string, ie.SessionOverrideOptions) error {
		if failed {
			return problem
		}
		return nil
	}}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn-test")
	for i := uint64(1); i <= 3; i++ {
		spec := newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled)
		spec.TriggerCount = i
		firing := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
		firing.Status, firing.TaskRunner, firing.Epoch = task.TaskStatus_Running, "cn-test", 1
		firing.ParentTaskID = fmt.Sprintf("sql-task:%d", sqlTask.TaskID)
		mustAddTestAsyncTask(t, store, 1, firing)
		firing = mustGetTestAsyncTask(t, store, 1, WithTaskMetadataId(EQ, firing.Metadata.ID))[0]
		if i == 3 {
			failed = false
		}
		err := executor.TaskExecutor()(ctx, &firing)
		if i < 3 {
			require.ErrorIs(t, err, problem)
		} else {
			require.NoError(t, err)
		}
	}
	require.Len(t, fake.execSQLs, 3)
	runs := mustGetTestSQLTaskRun(t, store, 3, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
	require.Equal(t, SQLTaskStatusFailed, runs[1].Status)
	require.Equal(t, SQLTaskStatusFailed, runs[2].Status)
}

func TestSQLTaskExecutorUnknownCommitErrorPaths(t *testing.T) {
	t.Run("gate", func(t *testing.T) {
		ctx := context.Background()
		store := NewMemTaskStorage().(*memTaskStorage)
		service := NewTaskService(runtime.DefaultRuntime(), store)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
			task.RetryLimit = 1
		})
		unknown := moerr.NewTxnUnknown(ctx, "gate-txn")
		fake := &fakeInternalExecutor{queryErrs: []error{fmt.Errorf("gate: %w", unknown)}}
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn-test")
		err := executor.ExecuteContext(ctx, newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
		require.ErrorIs(t, err, unknown)
		require.Len(t, fake.querySQLs, 1)
		require.Empty(t, fake.execSQLs)
		runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
		require.Equal(t, SQLTaskStatusRunning, runs[0].Status)
		require.False(t, runs[0].GateResult)
		require.Equal(t, int(moerr.ErrTxnUnknown), runs[0].ErrorCode)
		require.Contains(t, runs[0].ErrorMessage, "gate-txn")
	})

	t.Run("completion failure preserves unknown outcome", func(t *testing.T) {
		ctx := context.Background()
		baseStore := NewMemTaskStorage().(*memTaskStorage)
		store := &finishRunFailingTaskStorage{TaskStorage: baseStore}
		service := NewTaskService(runtime.DefaultRuntime(), store)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		sqlTask := mustAddSQLTaskForExecutorTest(t, baseStore, func(task *SQLTask) {
			task.GateCondition = ""
			task.RetryLimit = 1
		})
		unknown := moerr.NewTxnUnknown(ctx, "body-txn")
		fake := &fakeInternalExecutor{execErrs: []error{errors.Join(moerr.NewInternalErrorNoCtx("cleanup"), unknown)}}
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn-test")
		err := executor.ExecuteContext(ctx, newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
		require.ErrorIs(t, err, unknown)
		var identity *moerr.Error
		require.ErrorAs(t, err, &identity)
		require.Equal(t, moerr.ErrTxnUnknown, identity.ErrorCode())
		var pending *SQLTaskMetadataPendingError
		require.ErrorAs(t, err, &pending)
		require.True(t, pending.Recovering)
		require.Len(t, fake.execSQLs, 1)
	})
}

func TestSQLTaskExecutorOverlap(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select 1"
	})
	run := newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning)
	run.StartedAt = time.Now()
	mustAddTestSQLTaskRun(t, store, 1, run)

	fakeIE := &fakeInternalExecutor{}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.ErrorIs(t, err, ErrSQLTaskOverlap)
}

func TestSQLTaskExecutorAttachesAccountContext(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = "1"
		task.SQLBody = "select 1"
		task.AccountID = 7
		task.CreatorUserID = 11
		task.CreatorRoleID = 22
	})

	fakeIE := &fakeInternalExecutor{
		queryValues: []any{true},
		queryHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) {
			accountID, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Equal(t, sqlTask.AccountID, accountID)
			require.Equal(t, sqlTask.CreatorUserID, defines.GetUserId(ctx))
			require.Equal(t, sqlTask.CreatorRoleID, defines.GetRoleId(ctx))
		},
		execHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
			accountID, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Equal(t, sqlTask.AccountID, accountID)
			require.Equal(t, sqlTask.CreatorUserID, defines.GetUserId(ctx))
			require.Equal(t, sqlTask.CreatorRoleID, defines.GetRoleId(ctx))
			return nil
		},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.NoError(t, err)
}

func TestSQLTaskExecutorTaskExecutorPayloads(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, ts, "cn-test")
	require.Error(t, executor.TaskExecutor()(context.Background(), &task.CronTask{}))

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = ""
		task.SQLBody = ""
		task.TriggerCount = 1
	})
	mustAddTestSQLTaskRun(t, store, 1, newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning))
	spec := newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled)
	spec.TriggerCount = 1
	asyncTask := newTaskFromMetadata(BuildSQLTaskMetadata(spec))
	asyncTask.ParentTaskID = fmt.Sprintf("sql-task:%d", sqlTask.TaskID)
	asyncTask.Status, asyncTask.TaskRunner, asyncTask.Epoch = task.TaskStatus_Running, "cn-test", 1
	mustAddTestAsyncTask(t, store, 1, asyncTask)
	asyncTask = mustGetTestAsyncTask(t, store, 1)[0]
	require.NoError(t, executor.TaskExecutor()(context.Background(), &asyncTask))
	require.Eventually(t, func() bool {
		tasks, err := store.QueryAsyncTask(context.Background())
		return err == nil && len(tasks) == 1 && tasks[0].Status == task.TaskStatus_Completed
	}, time.Second, time.Millisecond)
}

func TestSQLTaskExecutorExecuteContextEdges(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, ts, "cn-test")
	require.Error(t, executor.ExecuteContext(context.Background(), nil, true))

	sqlTask := mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.GateCondition = ""
		task.SQLBody = "select"
		task.RetryLimit = 0
	})
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true)
	require.Error(t, err)
	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusFailed, runs[0].Status)

	sqlTask = mustAddSQLTaskForExecutorTest(t, store, func(task *SQLTask) {
		task.TaskName = "task-empty-body"
		task.GateCondition = ""
		task.SQLBody = ""
	})
	require.NoError(t, executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual), true))
}

func TestSQLTaskExecutorRejectsOversizedRunnerBeforeSQL(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := mustAddSQLTaskForExecutorTest(t, store, func(definition *SQLTask) {
		definition.SQLBody = "select 1"
		definition.TriggerCount = 1
	})
	fake := &fakeInternalExecutor{}
	factory := func() ie.InternalExecutor { return fake }
	manual := NewSQLTaskExecutor(factory, service, strings.Repeat("x", sqlTaskRunnerMaxBytes+1))
	err := manual.ExecuteContext(context.Background(), newSQLTaskContextForTest(definition, SQLTaskTriggerManual), true)
	require.ErrorContains(t, err, "execution identifier is too long")

	spec := newSQLTaskContextForTest(definition, SQLTaskTriggerScheduled)
	spec.TriggerCount = 1
	scheduled := NewSQLTaskExecutor(factory, service, "cn")
	for _, tooLong := range []string{"runner", "parent", "metadata"} {
		firing := &task.AsyncTask{ID: 1, Metadata: BuildSQLTaskMetadata(spec), ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
			Status: task.TaskStatus_Running, TaskRunner: "cn", Epoch: 1}
		switch tooLong {
		case "runner":
			firing.TaskRunner = strings.Repeat("x", sqlTaskAsyncRunnerMaxBytes+1)
		case "parent":
			firing.ParentTaskID = strings.Repeat("x", 51)
		case "metadata":
			firing.Metadata.ID = strings.Repeat("x", 51)
		}
		err = scheduled.TaskExecutor()(context.Background(), firing)
		require.ErrorContains(t, err, "execution identifier is too long", tooLong)
	}
	require.Empty(t, fake.querySQLs)
	require.Empty(t, fake.execSQLs)
	mustGetTestSQLTaskRun(t, store, 0)
}

func TestSQLTaskExecutorGateAndBoolEdges(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := newTestSQLTask("task-gate", 1)
	sqlTask.GateCondition = ""
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, ts, "cn-test")
	allowed, err := executor.evaluateGate(context.Background(), sqlTask)
	require.NoError(t, err)
	require.True(t, allowed)

	sqlTask.GateCondition = "select 1"
	executor = NewSQLTaskExecutor(func() ie.InternalExecutor {
		return &fakeInternalExecutor{queryErrs: []error{moerr.NewInternalErrorNoCtx("gate failed")}}
	}, ts, "cn-test")
	allowed, err = executor.evaluateGate(context.Background(), sqlTask)
	require.Error(t, err)
	require.False(t, allowed)

	executor = NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, ts, "cn-test")
	allowed, err = executor.evaluateGate(context.Background(), sqlTask)
	require.NoError(t, err)
	require.False(t, allowed)

	for _, value := range []any{true, int8(1), int16(1), int32(1), int64(1), int(1), uint8(1), uint16(1), uint32(1), uint64(1), uint(1), float32(1), float64(1), "yes", "t", "true", "1"} {
		got, err := toBool(value)
		require.NoError(t, err)
		require.True(t, got, "value %v", value)
	}
	for _, value := range []any{nil, false, int8(0), int16(0), int32(0), int64(0), int(0), uint8(0), uint16(0), uint32(0), uint64(0), uint(0), float32(0), float64(0), "", "0", "false", "f", "no", "n"} {
		got, err := toBool(value)
		require.NoError(t, err)
		require.False(t, got, "value %v", value)
	}
	_, err = toBool(struct{}{})
	require.Error(t, err)
}

func mustAddSQLTaskForExecutorTest(t *testing.T, store *memTaskStorage, adjust func(*SQLTask)) SQLTask {
	t.Helper()
	sqlTask := newTestSQLTask("task-exec", 1)
	sqlTask.GateCondition = "1"
	sqlTask.SQLBody = "select 1"
	sqlTask.DatabaseName = "db_exec"
	sqlTask.Creator = "executor_user"
	if adjust != nil {
		adjust(&sqlTask)
	}
	mustAddTestSQLTask(t, store, 1, sqlTask)
	return mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, sqlTask.TaskName))[0]
}

func newSQLTaskContextForTest(sqlTask SQLTask, triggerType string) *task.SQLTaskContext {
	return &task.SQLTaskContext{
		TaskId:         sqlTask.TaskID,
		TaskName:       sqlTask.TaskName,
		AccountId:      sqlTask.AccountID,
		DatabaseName:   sqlTask.DatabaseName,
		SQLBody:        sqlTask.SQLBody,
		GateCondition:  sqlTask.GateCondition,
		RetryLimit:     uint32(sqlTask.RetryLimit),
		TimeoutSeconds: uint32(sqlTask.TimeoutSeconds),
		Creator:        sqlTask.Creator,
		CreatorUserId:  sqlTask.CreatorUserID,
		CreatorRoleId:  sqlTask.CreatorRoleID,
		TriggerType:    triggerType,
		ScheduledAt:    time.Now().UnixMilli(),
		TriggerCount:   sqlTask.TriggerCount + 1,
	}
}

type fakeInternalExecutor struct {
	querySQLs   []string
	queryOpts   []ie.SessionOverrideOptions
	queryValues []any
	queryErrs   []error
	queryHook   func(context.Context, string, ie.SessionOverrideOptions)

	execSQLs         []string
	execOpts         []ie.SessionOverrideOptions
	execErrs         []error
	execAffectedRows []uint64
	execHook         func(context.Context, string, ie.SessionOverrideOptions) error
}

type finishRunFailingTaskStorage struct {
	TaskStorage
}

func (s *finishRunFailingTaskStorage) PublishSQLTaskReceipt(context.Context, SQLTaskReceiptRequest) (bool, error) {
	return false, moerr.NewInternalErrorNoCtx("finish failed")
}

func (f *fakeInternalExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	_, err := f.ExecWithStatus(ctx, sql, opts)
	return err
}

func (f *fakeInternalExecutor) ExecWithStatus(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (ie.InternalExecStatus, error) {
	f.execSQLs = append(f.execSQLs, sql)
	f.execOpts = append(f.execOpts, opts)
	if f.execHook != nil {
		return ie.InternalExecStatus{}, f.execHook(ctx, sql, opts)
	}
	status := ie.InternalExecStatus{}
	if len(f.execAffectedRows) > 0 {
		status.AffectedRows = f.execAffectedRows[0]
		f.execAffectedRows = f.execAffectedRows[1:]
	}
	if len(f.execErrs) == 0 {
		return status, nil
	}
	err := f.execErrs[0]
	f.execErrs = f.execErrs[1:]
	return status, err
}

func (f *fakeInternalExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	f.querySQLs = append(f.querySQLs, sql)
	f.queryOpts = append(f.queryOpts, opts)
	if f.queryHook != nil {
		f.queryHook(ctx, sql, opts)
	}
	result := &fakeInternalExecResult{}
	if len(f.queryErrs) > 0 {
		result.err = f.queryErrs[0]
		f.queryErrs = f.queryErrs[1:]
		return result
	}
	if len(f.queryValues) == 0 {
		return result
	}
	result.value = f.queryValues[0]
	f.queryValues = f.queryValues[1:]
	return result
}

func (f *fakeInternalExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

type fakeInternalExecResult struct {
	err   error
	value any
}

func (r *fakeInternalExecResult) Error() error { return r.err }

func (r *fakeInternalExecResult) ColumnCount() uint64 {
	if r.err != nil || r.value == nil {
		return 0
	}
	return 1
}

func (r *fakeInternalExecResult) Column(context.Context, uint64) (string, uint8, bool, error) {
	return "gate_result", 0, false, nil
}

func (r *fakeInternalExecResult) RowCount() uint64 {
	if r.err != nil || r.value == nil {
		return 0
	}
	return 1
}

func (r *fakeInternalExecResult) Row(context.Context, uint64) ([]interface{}, error) {
	if r.err != nil {
		return nil, r.err
	}
	return []interface{}{r.value}, nil
}

func (r *fakeInternalExecResult) Value(context.Context, uint64, uint64) (interface{}, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.value, nil
}

func (r *fakeInternalExecResult) GetUint64(context.Context, uint64, uint64) (uint64, error) {
	return 0, nil
}

func (r *fakeInternalExecResult) GetFloat64(context.Context, uint64, uint64) (float64, error) {
	return 0, nil
}

func (r *fakeInternalExecResult) GetString(context.Context, uint64, uint64) (string, error) {
	return "", nil
}
