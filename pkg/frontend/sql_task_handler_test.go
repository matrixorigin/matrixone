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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	taskpb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type sqlTaskContextKey struct{}

type unavailableSQLTaskCompletionStore struct{ taskservice.TaskStorage }

func (*unavailableSQLTaskCompletionStore) PublishSQLTaskReceipt(context.Context, taskservice.SQLTaskReceiptRequest) (bool, error) {
	return false, errors.New("injected metadata outage")
}

type falseSQLTaskGate struct{ ie.InternalExecResult }

func (falseSQLTaskGate) Error() error                                       { return nil }
func (falseSQLTaskGate) ColumnCount() uint64                                { return 1 }
func (falseSQLTaskGate) RowCount() uint64                                   { return 1 }
func (falseSQLTaskGate) Value(context.Context, uint64, uint64) (any, error) { return false, nil }

type falseGateExecutor struct{ captureExecOnlyIE }

func (*falseGateExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	return falseSQLTaskGate{}
}

func TestSQLTaskSkippedPendingWarning(t *testing.T) {
	ctx := context.Background()
	store := &unavailableSQLTaskCompletionStore{TaskStorage: taskservice.NewMemTaskStorage()}
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	definition := taskservice.SQLTask{TaskName: "skipped", AccountID: 1, GateCondition: "false", SQLBody: "insert into t values (1)"}
	_, err := store.AddSQLTask(ctx, definition)
	require.NoError(t, err)
	tasks, err := store.QuerySQLTask(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	fake := &falseGateExecutor{}
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn")
	err = executor.ExecuteContext(ctx, &taskpb.SQLTaskContext{TaskId: tasks[0].TaskID, TaskName: definition.TaskName,
		AccountId: definition.AccountID, GateCondition: definition.GateCondition, SQLBody: definition.SQLBody,
		TriggerType: taskservice.SQLTaskTriggerManual}, true)
	var pending *taskservice.SQLTaskMetadataPendingError
	require.ErrorAs(t, err, &pending)
	require.Equal(t, taskservice.SQLTaskStatusSkipped, pending.Status)
	require.Nil(t, pending.Cause)
	require.Empty(t, fake.execSQL, "the SQL body must not run when its condition is false")
	warning := sqlTaskPendingWarning(pending)
	require.Contains(t, warning, "skipped")
	require.Contains(t, warning, "SQL body was not executed")
	require.NotContains(t, warning, "succeeded")
}

func TestScheduledSQLTaskStatementErrorsAreRateLimited(t *testing.T) {
	service := t.Name()
	InitServerLevelVars(service)
	setPu(service, config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil))
	t.Cleanup(func() { serverVarsMap.Delete(service) })
	writer := &accountingMysqlWriter{}
	ses := NewSession(context.Background(), service, writer, nil)
	t.Cleanup(ses.Close)
	observed, logs := newObservedProtocolSession()
	ses.logger, ses.logLevel = observed.logger, observed.logLevel
	ses.loggerOnce.Do(func() {})
	background := context.WithValue(context.Background(), defines.SQLTaskBackgroundKey{}, true)
	problem := errors.New("scheduled failure")
	for i := 0; i < 100; i++ {
		logRolledBackStatement(background, ses, problem)
	}
	require.LessOrEqual(t, logs.Len(), 6)
	require.Equal(t, 100, writer.calls, "limiting logs must not skip statement accounting")
	other, otherLogs := newObservedProtocolSession()
	other.Error(background, "recover from panic")
	other.Error(background, "rollback whole txn on error failed")
	require.Equal(t, 2, otherLogs.Len(), "another task's critical diagnostics are independent")
	before := logs.Len()
	for range 4 {
		logRolledBackStatement(context.Background(), ses, problem)
	}
	require.Equal(t, before+8, logs.Len(), "ordinary client failures keep both existing logs")
}

func TestSQLTaskUnknownCommitProtocolIdentity(t *testing.T) {
	ctx := context.Background()
	store := taskservice.NewMemTaskStorage()
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	task := taskservice.SQLTask{TaskName: "unknown-commit", AccountID: 1, SQLBody: "insert into t values (1)", RetryLimit: 1}
	n, err := store.AddSQLTask(ctx, task)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	tasks, err := store.QuerySQLTask(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	unknown := moerr.NewTxnUnknown(ctx, "txn-1")
	fake := &captureExecOnlyIE{execErr: errors.Join(moerr.NewInternalErrorNoCtx("cleanup"), unknown)}
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn-test")
	err = executor.ExecuteContext(ctx, &taskpb.SQLTaskContext{
		TaskId: tasks[0].TaskID, TaskName: task.TaskName, AccountId: task.AccountID,
		SQLBody: task.SQLBody, RetryLimit: 1, TriggerType: taskservice.SQLTaskTriggerManual,
	}, true)
	require.ErrorIs(t, err, unknown)
	identity, onlyCanceled := queryErrorIdentity(err)
	require.False(t, onlyCanceled)
	require.NotNil(t, identity)
	require.Equal(t, moerr.ErrTxnUnknown, identity.ErrorCode())
}

func TestGetSQLTaskServicePublication(t *testing.T) {
	service := t.Name()
	InitServerLevelVars(service)
	t.Cleanup(func() { serverVarsMap.Delete(service) })
	pu := &config.ParameterUnit{}
	setPu(service, pu)
	ses := &Session{feSessionImpl: feSessionImpl{service: service}}
	ctx := context.Background()

	got, err := getSQLTaskService(ctx, ses)
	require.Nil(t, got)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.EqualError(t, err, "internal error: task service not ready yet, please try again later.")

	ts := &testTaskService{}
	start := make(chan struct{})
	published := make(chan struct{})
	go func() {
		<-start
		pu.SetTaskService(ts)
		close(published)
	}()
	defer func() { <-published }()

	// The publisher and this frontend read have no ordering until the join.
	// Either readiness result is valid; both accesses must use the same lock.
	close(start)
	got, err = getSQLTaskService(ctx, ses)
	if got == nil {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	} else {
		require.NoError(t, err)
		require.Same(t, ts, got)
	}

	<-published
	got, err = getSQLTaskService(ctx, ses)
	require.NoError(t, err)
	require.Same(t, ts, got)
}

// Observe the public handler's completion/fence ordering using the existing
// task store and query-client fixtures.
type sqlTaskFenceQueryClient struct {
	*mockQueryClient
	beforeSend func(context.Context)
}

func (c *sqlTaskFenceQueryClient) SendMessage(ctx context.Context, address string, req *querypb.Request) (*querypb.Response, error) {
	c.beforeSend(ctx)
	return c.mockQueryClient.SendMessage(ctx, address, req)
}

func TestHandleSQLTaskCreateAlterDrop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()

	createStmt := &tree.CreateSQLTask{
		Name:          tree.Identifier("task_a"),
		CronExpr:      "0 0 0 1 1 *",
		Timezone:      "Asia/Shanghai",
		GateCondition: "1",
		RetryLimit:    2,
		Timeout:       "45s",
		SQLBody:       "select 1;",
	}
	require.NoError(t, handleCreateSQLTask(context.Background(), ses, createStmt))

	sqlTask, err := getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, ses.GetDatabaseName(), sqlTask.DatabaseName)
	require.Equal(t, ses.GetTenantInfo().GetUserID(), sqlTask.CreatorUserID)
	require.Equal(t, ses.GetTenantInfo().GetDefaultRoleID(), sqlTask.CreatorRoleID)
	require.Greater(t, sqlTask.NextFireTime, int64(0))
	require.Equal(t, 45, sqlTask.TimeoutSeconds)

	require.NoError(t, handleAlterSQLTask(context.Background(), ses, &tree.AlterSQLTask{
		Name:   tree.Identifier("task_a"),
		Action: tree.AlterTaskSuspend,
	}))
	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.NoError(t, err)
	require.False(t, sqlTask.Enabled)

	require.NoError(t, handleAlterSQLTask(context.Background(), ses, &tree.AlterSQLTask{
		Name:   tree.Identifier("task_a"),
		Action: tree.AlterTaskResume,
	}))
	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.NoError(t, err)
	require.True(t, sqlTask.Enabled)
	require.Greater(t, sqlTask.NextFireTime, int64(1))

	require.NoError(t, handleAlterSQLTask(context.Background(), ses, &tree.AlterSQLTask{
		Name:     tree.Identifier("task_a"),
		Action:   tree.AlterTaskSetSchedule,
		CronExpr: "0 */5 * * * *",
		Timezone: "UTC",
	}))
	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, "0 */5 * * * *", sqlTask.CronExpr)
	require.Equal(t, "UTC", sqlTask.Timezone)
	require.Equal(t, uint64(3), sqlTask.TriggerCount)
	require.Equal(t, sqlTask.TriggerCount, sqlTask.ScheduleGeneration)

	// Model the async-only window deterministically: the scheduled child has
	// committed, but no executor has acquired it and therefore no run row can
	// preserve its account mapping. DROP TASK must remove the child together
	// with the last definition mapping.
	sqlTask.TriggerCount++
	parentTaskID := fmt.Sprintf("sql-task:%d", sqlTask.TaskID)
	queued := taskpb.AsyncTask{
		Metadata:     taskpb.TaskMetadata{ID: parentTaskID + ":1"},
		ParentTaskID: parentTaskID,
		Status:       taskpb.TaskStatus_Created,
	}
	affected, err := store.TriggerSQLTask(context.Background(), sqlTask, queued)
	require.NoError(t, err)
	require.Equal(t, 2, affected)
	children, err := store.QueryAsyncTask(context.Background(),
		taskservice.WithTaskParentTaskIDCond(taskservice.EQ, parentTaskID))
	require.NoError(t, err)
	require.Len(t, children, 1)
	runs, err := store.QuerySQLTaskRun(context.Background(),
		taskservice.WithTaskIDCond(taskservice.EQ, sqlTask.TaskID))
	require.NoError(t, err)
	require.Empty(t, runs)

	require.NoError(t, handleDropSQLTask(context.Background(), ses, &tree.DropSQLTask{
		Name: tree.Identifier("task_a"),
	}))
	_, err = getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.Error(t, err)
	children, err = store.QueryAsyncTask(context.Background(),
		taskservice.WithTaskParentTaskIDCond(taskservice.EQ, parentTaskID))
	require.NoError(t, err)
	require.Empty(t, children)

	require.NoError(t, ts.Close())
}

func TestHandleCreateSQLTaskRequiresDatabase(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	ctx := context.Background()

	require.NoError(t, handleCreateSQLTask(ctx, ses, &tree.CreateSQLTask{
		Name:    tree.Identifier("task_existing"),
		SQLBody: "select 1",
	}))
	before := mustGetTestSQLTasksForFrontend(t, store)
	require.Len(t, before, 1)
	require.Equal(t, "testdb", before[0].DatabaseName)
	ses.SetDatabaseName("")

	checkNoDatabase := func(t *testing.T, stmt *tree.CreateSQLTask) {
		t.Helper()
		err := handleCreateSQLTask(ctx, ses, stmt)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoDB), "got %v", err)
		var dbErr *moerr.Error
		require.ErrorAs(t, err, &dbErr)
		require.Equal(t, uint16(1046), dbErr.MySQLCode())
		require.Equal(t, "3D000", dbErr.SqlState())
		require.EqualError(t, err, "No database selected")
		require.Equal(t, before, mustGetTestSQLTasksForFrontend(t, store))
		runs, err := store.QuerySQLTaskRun(ctx)
		require.NoError(t, err)
		require.Empty(t, runs)
	}

	for _, tc := range []struct {
		name string
		stmt tree.CreateSQLTask
	}{
		{
			name: "unqualified body",
			stmt: tree.CreateSQLTask{Name: "task_unqualified", SQLBody: "insert into tmk values (1)"},
		},
		{
			name: "qualified body",
			stmt: tree.CreateSQLTask{Name: "task_qualified", SQLBody: "insert into testdb.tmk values (1)"},
		},
		{
			name: "constant body",
			stmt: tree.CreateSQLTask{Name: "task_constant", SQLBody: "select 1"},
		},
		{
			name: "scheduled task with gate",
			stmt: tree.CreateSQLTask{
				Name: "task_scheduled", CronExpr: "0 0 0 1 1 *", Timezone: "UTC",
				GateCondition: "1", SQLBody: "select 1",
			},
		},
		{
			name: "existing task",
			stmt: tree.CreateSQLTask{Name: "task_existing", SQLBody: "select 2"},
		},
		{
			name: "existing task with if not exists",
			stmt: tree.CreateSQLTask{Name: "task_existing", IfNotExists: true, SQLBody: "select 2"},
		},
		{
			name: "invalid timeout",
			stmt: tree.CreateSQLTask{Name: "task_bad_timeout", Timeout: "-1s", SQLBody: "select 1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkNoDatabase(t, &tc.stmt)
		})
	}

	t.Run("task service not published", func(t *testing.T) {
		pu := getPu(ses.GetService())
		pu.SetTaskService(nil)
		t.Cleanup(func() { pu.SetTaskService(ts) })
		checkNoDatabase(t, &tree.CreateSQLTask{Name: "task_not_ready", SQLBody: "select 1"})
	})
}

func TestHandleSQLTaskDefaultsTimezoneToUTC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	require.NoError(t, handleCreateSQLTask(context.Background(), ses, &tree.CreateSQLTask{
		Name:     tree.Identifier("task_default_tz"),
		CronExpr: "0 0 0 1 1 *",
		SQLBody:  "select 1;",
	}))

	sqlTask, err := getSQLTaskByName(context.Background(), store, "task_default_tz", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, "UTC", sqlTask.Timezone)

	require.NoError(t, handleAlterSQLTask(context.Background(), ses, &tree.AlterSQLTask{
		Name:     tree.Identifier("task_default_tz"),
		Action:   tree.AlterTaskSetSchedule,
		CronExpr: "0 */5 * * * *",
	}))

	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_default_tz", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, "UTC", sqlTask.Timezone)
	require.Equal(t, "0 */5 * * * *", sqlTask.CronExpr)
}

type sqlTaskShowTestExecutor struct{ captureExecOnlyIE }

func (*sqlTaskShowTestExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	return &cdcStateQueryResult{state: "0", rows: 1}
}

func runSQLTaskForShowTest(t *testing.T, service taskservice.TaskService, definition taskservice.SQLTask, trigger string, skip bool) {
	t.Helper()
	spec := &taskpb.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
		DatabaseName: definition.DatabaseName, SQLBody: definition.SQLBody, TriggerType: trigger, ScheduledAt: time.Now().UnixMilli()}
	if skip {
		spec.GateCondition = "0"
	}
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return &sqlTaskShowTestExecutor{} }, service, "cn1")
	require.NoError(t, executor.ExecuteContext(context.Background(), spec, true))
}

func TestHandleShowSQLTasksAndRuns(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	now := time.Now()
	sqlTask := taskservice.SQLTask{
		TaskName:       "task_show",
		AccountID:      ses.GetAccountId(),
		DatabaseName:   ses.GetDatabaseName(),
		CronExpr:       "0 0 0 1 1 *",
		Timezone:       "UTC",
		SQLBody:        "select 1",
		GateCondition:  "1",
		RetryLimit:     1,
		TimeoutSeconds: 30,
		Enabled:        true,
		NextFireTime:   now.Add(time.Hour).UnixMilli(),
		Creator:        ses.GetUserName(),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err := store.AddSQLTask(context.Background(), sqlTask)
	require.NoError(t, err)
	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_show", ses.GetAccountId())
	require.NoError(t, err)

	runSQLTaskForShowTest(t, ts, sqlTask, taskservice.SQLTaskTriggerManual, false)
	runSQLTaskForShowTest(t, ts, sqlTask, taskservice.SQLTaskTriggerManual, true)

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTasks(context.Background(), ses, nil, &tree.ShowSQLTasks{}))
	require.Equal(t, uint64(9), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())
	row, err := ses.mrs.GetRow(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, "task_show", row[0])
	require.Equal(t, "CRON_TZ=UTC 0 0 0 1 1 *", row[1])
	require.Equal(t, int32(1), row[2])
	require.Equal(t, int32(1), row[4])
	require.Equal(t, taskservice.SQLTaskStatusSkipped, row[7])

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTaskRuns(context.Background(), ses, nil, &tree.ShowSQLTaskRuns{
		TaskName: tree.Identifier("task_show"),
		HasTask:  true,
		Limit:    1,
		HasLimit: true,
	}))
	require.Equal(t, uint64(11), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())
	row, err = ses.mrs.GetRow(context.Background(), 0)
	require.NoError(t, err)
	_, ok := row[0].(int64)
	require.True(t, ok)
	require.Equal(t, "task_show", row[1])
	require.Equal(t, taskservice.SQLTaskTriggerManual, row[2])
	require.Equal(t, taskservice.SQLTaskStatusSkipped, row[3])
	require.Equal(t, int32(1), row[7])
	require.Equal(t, "cn1", row[10])
}

func TestHandleShowSQLTasksAndRunsWithSavedQueryResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	require.NoError(t, ses.SetSessionSysVar(context.Background(), "save_query_result", int8(1)))

	now := time.Now()
	sqlTask := taskservice.SQLTask{
		TaskName:       "task_saved_result",
		AccountID:      ses.GetAccountId(),
		DatabaseName:   ses.GetDatabaseName(),
		CronExpr:       "0 0 0 1 1 *",
		Timezone:       "UTC",
		SQLBody:        "select 1",
		GateCondition:  "1",
		RetryLimit:     1,
		TimeoutSeconds: 30,
		Enabled:        true,
		NextFireTime:   now.Add(time.Hour).UnixMilli(),
		Creator:        ses.GetUserName(),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err := store.AddSQLTask(context.Background(), sqlTask)
	require.NoError(t, err)
	sqlTask, err = getSQLTaskByName(context.Background(), store, "task_saved_result", ses.GetAccountId())
	require.NoError(t, err)

	runSQLTaskForShowTest(t, ts, sqlTask, taskservice.SQLTaskTriggerManual, false)

	showTasks, err := parsers.Parse(context.Background(), dialect.MYSQL, "show tasks", 1)
	require.NoError(t, err)
	ses.ast = showTasks[0]
	ses.SetStmtId(uuid.New())
	ses.SetSqlSourceType(constant.CloudUserSql)

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTasks(context.Background(), ses, nil, &tree.ShowSQLTasks{}))
	require.NotNil(t, ses.rs)

	showTaskRuns, err := parsers.Parse(context.Background(), dialect.MYSQL, "show task runs for task_saved_result limit 1", 1)
	require.NoError(t, err)
	ses.ast = showTaskRuns[0]
	ses.SetStmtId(uuid.New())
	ses.SetSqlSourceType(constant.CloudUserSql)

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTaskRuns(context.Background(), ses, nil, &tree.ShowSQLTaskRuns{
		TaskName: tree.Identifier("task_saved_result"),
		HasTask:  true,
		Limit:    1,
		HasLimit: true,
	}))
	require.NotNil(t, ses.rs)
}

func TestHandleSQLTaskEdges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	ctx := context.Background()
	createStmt := &tree.CreateSQLTask{
		Name:    tree.Identifier("task_edge"),
		SQLBody: "select 1",
	}
	require.NoError(t, handleCreateSQLTask(ctx, ses, createStmt))
	require.NoError(t, handleCreateSQLTask(ctx, ses, &tree.CreateSQLTask{
		IfNotExists: true,
		Name:        tree.Identifier("task_edge"),
	}))
	require.Len(t, mustGetTestSQLTasksForFrontend(t, store, taskservice.WithTaskName(taskservice.EQ, "task_edge")), 1)
	require.Error(t, handleCreateSQLTask(ctx, ses, createStmt))
	require.Error(t, handleCreateSQLTask(ctx, ses, &tree.CreateSQLTask{
		Name:    tree.Identifier("task_bad_timeout"),
		Timeout: "-1s",
	}))

	require.NoError(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:          tree.Identifier("task_edge"),
		Action:        tree.AlterTaskSetWhen,
		GateCondition: "select 1",
	}))
	sqlTask, err := getSQLTaskByName(ctx, store, "task_edge", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, "select 1", sqlTask.GateCondition)

	require.NoError(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:       tree.Identifier("task_edge"),
		Action:     tree.AlterTaskSetRetry,
		RetryLimit: 3,
	}))
	sqlTask, err = getSQLTaskByName(ctx, store, "task_edge", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, 3, sqlTask.RetryLimit)

	require.NoError(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:    tree.Identifier("task_edge"),
		Action:  tree.AlterTaskSetTimeout,
		Timeout: "2m",
	}))
	sqlTask, err = getSQLTaskByName(ctx, store, "task_edge", ses.GetAccountId())
	require.NoError(t, err)
	require.Equal(t, 120, sqlTask.TimeoutSeconds)

	require.Error(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:    tree.Identifier("task_edge"),
		Action:  tree.AlterTaskSetTimeout,
		Timeout: "bad",
	}))
	require.Error(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:     tree.Identifier("task_edge"),
		Action:   tree.AlterTaskSetSchedule,
		CronExpr: "bad cron",
	}))
	require.Error(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{
		Name:   tree.Identifier("missing"),
		Action: tree.AlterTaskSuspend,
	}))

	require.NoError(t, handleDropSQLTask(ctx, ses, &tree.DropSQLTask{
		IfExists: true,
		Name:     tree.Identifier("missing"),
	}))
	require.Error(t, handleDropSQLTask(ctx, ses, &tree.DropSQLTask{
		Name: tree.Identifier("missing"),
	}))
}

func TestHandleSQLTaskMutationsSyncCommitTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	mockQC := newMockQueryClient()
	mockTC := &mockTxnClient{}
	ses.proc.Base.QueryClient = mockQC
	ses.proc.Base.TxnClient = mockTC

	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{
			{QueryAddress: "cn1:6001"},
			{QueryAddress: "cn2:6001"},
			{QueryAddress: "cn3:6001"},
		},
	}
	runtime.SetupServiceBasedRuntime("test-service", runtime.DefaultRuntime())
	runtime.ServiceRuntime("test-service").SetGlobalVariables(runtime.ClusterService, mockMC)

	ts1 := newTestTimestamp(100, 1)
	ts2 := newTestTimestamp(100, 2)
	ts3 := newTestTimestamp(99, 100)
	mockQC.cnResponses["cn1:6001"] = &querypb.Response{GetCommit: &querypb.GetCommitResponse{CurrentCommitTS: ts1}}
	mockQC.cnResponses["cn2:6001"] = &querypb.Response{GetCommit: &querypb.GetCommitResponse{CurrentCommitTS: ts2}}
	mockQC.cnResponses["cn3:6001"] = &querypb.Response{GetCommit: &querypb.GetCommitResponse{CurrentCommitTS: ts3}}

	ctx := context.Background()
	require.NoError(t, handleCreateSQLTask(ctx, ses, &tree.CreateSQLTask{
		Name:    tree.Identifier("task_sync_commit"),
		SQLBody: "",
	}))
	require.True(t, mockTC.syncCalled)
	require.Equal(t, ts2, mockTC.syncCalledWith)
	require.Equal(t, 3, mockQC.sendCalls)
	require.Equal(t, 3, mockQC.releaseCalls)

	mockTC.syncCalled = false
	mockTC.syncCalledWith = newTestTimestamp(0, 0)
	mockQC.sendCalls = 0
	mockQC.releaseCalls = 0
	mockQC.newReqCalls = 0

	require.NoError(t, handleExecuteSQLTask(ctx, ses, &tree.ExecuteSQLTask{Name: tree.Identifier("task_sync_commit")}))
	require.True(t, mockTC.syncCalled)
	require.Equal(t, ts2, mockTC.syncCalledWith)
	require.Equal(t, 3, mockQC.sendCalls)
	require.Equal(t, 3, mockQC.releaseCalls)
	// A gate parse failure still commits the run's terminal state before the
	// handler returns. Its error must survive the same cross-CN fence.
	task, err := getSQLTaskByName(ctx, store, "task_sync_commit", ses.GetAccountId())
	require.NoError(t, err)
	invalidGate := task
	invalidGate.GateCondition = "("
	_, err = store.AlterSQLTask(ctx, task, invalidGate, taskservice.SQLTaskSetGate)
	require.NoError(t, err)
	mockTC.syncCalled = false
	mockQC.sendCalls = 0
	mockQC.releaseCalls = 0
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	ses.proc.Base.QueryClient = &sqlTaskFenceQueryClient{
		mockQueryClient: mockQC,
		beforeSend: func(rpcCtx context.Context) {
			runs, queryErr := store.QuerySQLTaskRun(ctx, taskservice.WithTaskIDCond(taskservice.EQ, task.TaskID))
			require.NoError(t, queryErr)
			require.Len(t, runs, 2)
			require.Equal(t, taskservice.SQLTaskStatusFailed, runs[0].Status)
			// Cancel after completion: the already-owned execution still needs
			// a live, bounded visibility RPC to publish its final metadata.
			cancelRequest()
			require.NoError(t, rpcCtx.Err())
			_, bounded := rpcCtx.Deadline()
			require.True(t, bounded)
		},
	}
	err = handleExecuteSQLTask(requestCtx, ses, &tree.ExecuteSQLTask{Name: tree.Identifier("task_sync_commit")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "syntax")
	runs, queryErr := store.QuerySQLTaskRun(ctx, taskservice.WithTaskIDCond(taskservice.EQ, task.TaskID))
	require.NoError(t, queryErr)
	require.Len(t, runs, 2)
	require.Equal(t, taskservice.SQLTaskStatusFailed, runs[0].Status)
	require.True(t, mockTC.syncCalled)
	require.Equal(t, ts2, mockTC.syncCalledWith)
	require.Equal(t, 3, mockQC.sendCalls)
	require.Equal(t, 3, mockQC.releaseCalls)

	// A failed CN must not prevent syncing from the other responses.
	originalIdentity, originalCanceled := queryErrorIdentity(err)
	mockQC.sendCalls, mockQC.releaseCalls = 0, 0
	mockQC.sendError["cn1:6001"] = errors.New("injected query failure")
	mockTC.syncCalled = false
	ses.proc.Base.QueryClient = mockQC
	beforeFailures := testutil.ToFloat64(v2.SQLTaskSyncCommitFailureCounter)
	require.Error(t, syncSQLTaskMetadataCommitTimestamp(ctx, ses))
	require.Equal(t, 3, mockQC.sendCalls)
	require.Equal(t, 2, mockQC.releaseCalls)
	require.True(t, mockTC.syncCalled)
	require.Equal(t, ts2, mockTC.syncCalledWith)
	require.GreaterOrEqual(t, testutil.ToFloat64(v2.SQLTaskSyncCommitFailureCounter)-beforeFailures, float64(1))

	// An execution error plus an incomplete visibility fence must retain the
	// business error's public identity while reporting the visibility failure.
	err = handleExecuteSQLTask(ctx, ses, &tree.ExecuteSQLTask{Name: tree.Identifier("task_sync_commit")})
	require.ErrorContains(t, err, "syntax")
	require.ErrorContains(t, err, "task metadata visibility could not be confirmed")
	identity, onlyCanceled := queryErrorIdentity(err)
	require.Equal(t, originalCanceled, onlyCanceled)
	if originalIdentity == nil {
		require.Nil(t, identity)
	} else {
		require.NotNil(t, identity)
		require.Equal(t, originalIdentity.ErrorCode(), identity.ErrorCode())
	}

	// A response can accompany an RPC error and still belongs to the client.
	mockQC.responseWithError = true
	mockQC.sendCalls, mockQC.releaseCalls = 0, 0
	warnings := ses.warningCount()
	warnSQLTaskVisibility(ctx, ses)
	require.Equal(t, 3, mockQC.sendCalls)
	require.Equal(t, 3, mockQC.releaseCalls)
	require.Equal(t, warnings+1, ses.warningCount())

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	mockQC.sendCalls = 0
	require.ErrorIs(t, syncSQLTaskMetadataCommitTimestamp(canceled, ses), context.Canceled)
	require.Zero(t, mockQC.sendCalls)
}

func TestExecInFrontendSQLTaskStatements(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses, ts, _ := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	defer func() {
		require.NoError(t, ts.Close())
	}()

	ctx := context.Background()
	run := func(stmt tree.Statement) error {
		execCtx := &ExecCtx{
			reqCtx: ctx,
			stmt:   stmt,
			ses:    ses,
		}
		defer execCtx.Close()
		_, err := execInFrontend(ses, execCtx)
		return err
	}

	require.NoError(t, run(&tree.CreateSQLTask{
		Name:    tree.Identifier("task_frontend"),
		SQLBody: "",
	}))
	require.NoError(t, run(&tree.AlterSQLTask{
		Name:   tree.Identifier("task_frontend"),
		Action: tree.AlterTaskSuspend,
	}))
	require.NoError(t, run(&tree.ExecuteSQLTask{Name: tree.Identifier("task_frontend")}))

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, run(&tree.ShowSQLTasks{}))
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, run(&tree.ShowSQLTaskRuns{TaskName: tree.Identifier("task_frontend"), HasTask: true}))
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())

	require.NoError(t, run(&tree.DropSQLTask{Name: tree.Identifier("task_frontend")}))
}

func TestSQLTaskStatementsRequireAdminPrivilege(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()
	stmts := []tree.Statement{
		&tree.CreateSQLTask{Name: tree.Identifier("task_auth"), SQLBody: "select 1"},
		&tree.AlterSQLTask{Name: tree.Identifier("task_auth"), Action: tree.AlterTaskSuspend},
		&tree.AlterSQLTask{Name: tree.Identifier("task_auth"), Action: tree.AlterTaskResetRun, RunID: 1},
		&tree.DropSQLTask{Name: tree.Identifier("task_auth")},
		&tree.ExecuteSQLTask{Name: tree.Identifier("task_auth")},
	}

	for _, stmt := range stmts {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		require.NoError(t, err)
		require.True(t, ok)
	}

	ses.SetTenantInfo(&TenantInfo{
		Tenant:        "tenant1",
		User:          "u1",
		DefaultRole:   "developer",
		TenantID:      2,
		UserID:        20,
		DefaultRoleID: 30,
	})
	for _, stmt := range stmts {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestSQLTaskForceRecoveryHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses, _, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()
	definition := taskservice.SQLTask{TaskName: "owned", AccountID: ses.GetAccountId(), Enabled: true, ScheduleGeneration: 1}
	_, err := store.AddSQLTask(ctx, definition)
	require.NoError(t, err)
	tasks, err := store.QuerySQLTask(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	definition = tasks[0]
	run := taskservice.SQLTaskRun{TaskID: definition.TaskID, TaskName: definition.TaskName, AccountID: definition.AccountID,
		Status: taskservice.SQLTaskStatusRunning, TriggerType: taskservice.SQLTaskTriggerManual, RunnerCN: "cn-owner", AttemptNumber: 1}
	runID, err := store.AcquireSQLTaskRun(ctx, definition, run, &taskservice.SQLTaskRunAdmission{Reserve: func(uint32, string) bool { return true }})
	require.NoError(t, err)
	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTaskRuns(ctx, ses, nil, &tree.ShowSQLTaskRuns{}))
	row, err := ses.mrs.GetRow(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, row[9])
	require.Equal(t, "cn-owner", row[10])
	warnings := ses.warningCount()
	stmt := &tree.AlterSQLTask{Name: tree.Identifier(definition.TaskName), Action: tree.AlterTaskResetRun, RunID: int64(runID)}
	require.NoError(t, handleAlterSQLTask(ctx, ses, stmt))
	require.Greater(t, ses.warningCount(), warnings)
	runs, err := store.QuerySQLTaskRun(ctx)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, taskservice.SQLTaskStatusUnknown, runs[0].Status)

	// A dropped definition still permits the tenant admin to inspect/replay
	// the exact recovery without accidentally affecting another run.
	_, err = store.DeleteSQLTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, definition.TaskID))
	require.NoError(t, err)
	require.NoError(t, handleAlterSQLTask(ctx, ses, stmt))
}

func TestDetachSQLTaskExecuteContext(t *testing.T) {
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), sqlTaskContextKey{}, "value"), time.Millisecond)
	defer cancel()

	child := detachSQLTaskExecuteContext(parent)
	cancel()

	_, ok := child.Deadline()
	require.False(t, ok)
	require.NoError(t, child.Err())
	require.Equal(t, "value", child.Value(sqlTaskContextKey{}))
}

func newSQLTaskHandlerTestSession(t *testing.T, ctrl *gomock.Controller) (*Session, taskservice.TaskService, taskservice.TaskStorage) {
	t.Helper()
	ses := newTestSession(t, ctrl)
	require.NoError(t, ses.SetSessionSysVar(context.Background(), "save_query_result", int8(0)))
	ses.SetDatabaseName("testdb")

	store := taskservice.NewMemTaskStorage()
	ts := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() {
		require.NoError(t, ts.Close())
	})
	getPu(ses.GetService()).TaskService = ts
	return ses, ts, store
}

func mustGetTestSQLTasksForFrontend(t *testing.T, store taskservice.TaskStorage, conds ...taskservice.Condition) []taskservice.SQLTask {
	t.Helper()
	tasks, err := store.QuerySQLTask(context.Background(), conds...)
	require.NoError(t, err)
	return tasks
}

// Inject a committed scheduler firing between the handler's read and update.
type triggerBeforeAlterStore struct {
	taskservice.TaskStorage
	triggered bool
}

func (s *triggerBeforeAlterStore) AlterSQLTask(ctx context.Context, observed, updated taskservice.SQLTask, action taskservice.SQLTaskAlterAction) (int, error) {
	if !s.triggered {
		s.triggered = true
		trigger := observed
		trigger.TriggerCount++
		async := taskpb.AsyncTask{Metadata: taskpb.TaskMetadata{ID: "alter-trigger"}}
		n, err := s.TaskStorage.TriggerSQLTask(ctx, trigger, async)
		if err != nil {
			return 0, err
		}
		if n != 2 {
			return 0, fmt.Errorf("trigger did not commit: affected %d", n)
		}
	}
	return s.TaskStorage.AlterSQLTask(ctx, observed, updated, action)
}

type alterTestTaskService struct {
	taskservice.TaskService
	store taskservice.TaskStorage
}

func (s *alterTestTaskService) GetStorage() taskservice.TaskStorage { return s.store }

func TestHandleSQLTaskSuspendAfterTrigger(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses, service, store := newSQLTaskHandlerTestSession(t, ctrl)
	defer ses.Close()
	ctx := t.Context()
	require.NoError(t, handleCreateSQLTask(ctx, ses, &tree.CreateSQLTask{Name: tree.Identifier("suspend_after_trigger"), CronExpr: "0 0 0 1 1 *", Timezone: "UTC", SQLBody: "select 1;"}))
	wrapper := &triggerBeforeAlterStore{TaskStorage: store}
	getPu(ses.GetService()).TaskService = &alterTestTaskService{TaskService: service, store: wrapper}
	require.NoError(t, handleAlterSQLTask(ctx, ses, &tree.AlterSQLTask{Name: tree.Identifier("suspend_after_trigger"), Action: tree.AlterTaskSuspend}))
	require.True(t, wrapper.triggered)
	got, err := getSQLTaskByName(ctx, store, "suspend_after_trigger", ses.GetAccountId())
	require.NoError(t, err)
	require.False(t, got.Enabled)
	require.Equal(t, got.ScheduleGeneration, got.TriggerCount)
}
