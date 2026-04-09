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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

type sqlTaskContextKey struct{}

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

	sqlTask.NextFireTime = 1
	sqlTask.TriggerCount = 5
	_, err = store.UpdateSQLTask(context.Background(), []taskservice.SQLTask{sqlTask}, taskservice.WithTaskIDCond(taskservice.EQ, sqlTask.TaskID))
	require.NoError(t, err)

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
	require.Equal(t, uint64(0), sqlTask.TriggerCount)

	require.NoError(t, handleDropSQLTask(context.Background(), ses, &tree.DropSQLTask{
		Name: tree.Identifier("task_a"),
	}))
	_, err = getSQLTaskByName(context.Background(), store, "task_a", ses.GetAccountId())
	require.Error(t, err)

	require.NoError(t, ts.Close())
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

	run := taskservice.SQLTaskRun{
		TaskID:          sqlTask.TaskID,
		TaskName:        sqlTask.TaskName,
		AccountID:       sqlTask.AccountID,
		StartedAt:       now,
		FinishedAt:      now.Add(2 * time.Second),
		DurationSeconds: 2,
		Status:          taskservice.SQLTaskStatusSuccess,
		TriggerType:     taskservice.SQLTaskTriggerScheduled,
		AttemptNumber:   1,
		GateResult:      true,
		RunnerCN:        "cn1",
	}
	_, err = store.AddSQLTaskRun(context.Background(), run)
	require.NoError(t, err)

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTasks(context.Background(), ses, nil, &tree.ShowSQLTasks{}))
	require.Equal(t, uint64(9), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())
	row, err := ses.mrs.GetRow(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, "task_show", row[0])
	require.Equal(t, "CRON_TZ=UTC 0 0 0 1 1 *", row[1])
	require.Equal(t, taskservice.SQLTaskStatusSuccess, row[7])

	ses.mrs = &MysqlResultSet{}
	require.NoError(t, handleShowSQLTaskRuns(context.Background(), ses, nil, &tree.ShowSQLTaskRuns{
		TaskName: tree.Identifier("task_show"),
		HasTask:  true,
		Limit:    1,
		HasLimit: true,
	}))
	require.Equal(t, uint64(10), ses.mrs.GetColumnCount())
	require.Equal(t, uint64(1), ses.mrs.GetRowCount())
	row, err = ses.mrs.GetRow(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, "task_show", row[1])
	require.Equal(t, taskservice.SQLTaskTriggerScheduled, row[2])
	require.Equal(t, taskservice.SQLTaskStatusSuccess, row[3])
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
	getPu(ses.GetService()).TaskService = ts
	return ses, ts, store
}
