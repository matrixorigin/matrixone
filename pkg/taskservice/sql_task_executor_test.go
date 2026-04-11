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

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusSuccess, runs[0].Status)
	require.True(t, runs[0].GateResult)
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
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled), true)
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
		task.RetryLimit = 1
	})

	fakeIE := &fakeInternalExecutor{
		queryErrs: []error{moerr.NewInternalErrorNoCtx("gate failed")},
	}
	executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return fakeIE }, ts, "cn-test")
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled), true)
	require.EqualError(t, err, "internal error: finish failed")
	require.Equal(t, 1, store.completeCalls)

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
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled), true)
	require.NoError(t, err)

	runs := mustGetTestSQLTaskRun(t, store, 2, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusFailed, runs[0].Status)
	require.Equal(t, SQLTaskStatusSuccess, runs[1].Status)
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
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled), true)
	require.Error(t, err)

	runs := mustGetTestSQLTaskRun(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))
	require.Equal(t, SQLTaskStatusTimeout, runs[0].Status)
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
	mustAddTestSQLTaskRun(t, store, 1, newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning))

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
	err := executor.ExecuteContext(context.Background(), newSQLTaskContextForTest(sqlTask, SQLTaskTriggerScheduled), true)
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
	})
	mustAddTestSQLTaskRun(t, store, 1, newTestSQLTaskRun(sqlTask.TaskID, sqlTask.TaskName, SQLTaskStatusRunning))
	asyncTask := newTaskFromMetadata(BuildSQLTaskMetadata(newSQLTaskContextForTest(sqlTask, SQLTaskTriggerManual)))
	require.NoError(t, executor.TaskExecutor()(context.Background(), &asyncTask))
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

	execSQLs []string
	execOpts []ie.SessionOverrideOptions
	execErrs []error
	execHook func(context.Context, string, ie.SessionOverrideOptions) error
}

type finishRunFailingTaskStorage struct {
	TaskStorage
	completeCalls int
}

func (s *finishRunFailingTaskStorage) CompleteSQLTaskRun(context.Context, SQLTaskRun) (int, error) {
	s.completeCalls++
	return 0, moerr.NewInternalErrorNoCtx("finish failed")
}

func (f *fakeInternalExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	f.execSQLs = append(f.execSQLs, sql)
	f.execOpts = append(f.execOpts, opts)
	if f.execHook != nil {
		return f.execHook(ctx, sql, opts)
	}
	if len(f.execErrs) == 0 {
		return nil
	}
	err := f.execErrs[0]
	f.execErrs = f.execErrs[1:]
	return err
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
