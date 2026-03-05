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
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util/json"
)

func TestBuildWhereClause(t *testing.T) {
	cases := []struct {
		condition conditions

		expected string
	}{
		{
			condition: conditions(map[condCode]condition{CondTaskID: &taskIDCond{op: EQ, taskID: 1}}),
			expected:  " AND task_id=1",
		},
		{
			condition: conditions(
				map[condCode]condition{
					CondTaskID:           &taskIDCond{op: EQ, taskID: 1},
					CondTaskRunner:       &taskRunnerCond{op: EQ, taskRunner: "abc"},
					CondTaskStatus:       &taskStatusCond{op: IN, taskStatus: []task.TaskStatus{task.TaskStatus_Created}},
					CondTaskEpoch:        &taskEpochCond{op: LE, taskEpoch: 100},
					CondTaskParentTaskID: &taskParentTaskIDCond{op: GE, taskParentTaskID: "ab"},
					CondTaskExecutor:     &taskExecutorCond{op: GE, taskExecutor: 1},
				},
			),
			expected: " AND task_id=1 AND task_runner='abc' AND task_status IN (0) AND task_epoch<=100 AND task_parent_id>='ab' AND task_metadata_executor>=1",
		},
		{
			condition: conditions(map[condCode]condition{
				CondTaskRunner:       &taskRunnerCond{op: EQ, taskRunner: "abc"},
				CondTaskStatus:       &taskStatusCond{op: IN, taskStatus: []task.TaskStatus{task.TaskStatus_Created}},
				CondTaskParentTaskID: &taskParentTaskIDCond{op: GE, taskParentTaskID: "ab"},
				CondTaskExecutor:     &taskExecutorCond{op: LE, taskExecutor: 1},
			},
			),
			expected: " AND task_runner='abc' AND task_status IN (0) AND task_parent_id>='ab' AND task_metadata_executor<=1",
		},
	}

	for _, c := range cases {
		result := buildWhereClause(&c.condition)
		actual := strings.Split(result, " AND ")
		expected := strings.Split(c.expected, " AND ")
		slices.Sort(actual)
		slices.Sort(expected)
		assert.Equal(t, expected, actual)
	}
}

var (
	asyncRows = []string{
		"task_id",
		"task_metadata_id",
		"task_metadata_executor",
		"task_metadata_context",
		"task_metadata_option",
		"task_parent_id",
		"task_status",
		"task_runner",
		"task_epoch",
		"last_heartbeat",
		"result_code",
		"error_msg",
		"create_at",
		"end_at"}

	cronRows = []string{
		"task_id",
		"task_metadata_id",
		"task_metadata_executor",
		"task_metadata_context",
		"task_metadata_option",
		"cron_expr",
		"next_time",
		"trigger_times",
		"create_at",
		"update_at",
	}

	daemonHeads = []string{
		"task_id",
		"task_metadata_id",
		"task_metadata_executor",
		"task_metadata_context",
		"task_metadata_option",
		"account_id",
		"account",
		"task_type",
		"task_runner",
		"task_status",
		"last_heartbeat",
		"create_at",
		"update_at",
		"end_at",
		"last_run",
		"details",
	}

	daemonRows = []string{
		"task_id",
		"task_metadata_id",
		"task_metadata_executor",
		"task_metadata_context",
		"task_metadata_option",
		"account_id",
		"account",
		"task_type",
		"task_runner",
		"task_status",
		"last_heartbeat",
		"create_at",
		"update_at",
		"end_at",
		"last_run",
		"details",
	}
)

func TestPingContext(t *testing.T) {
	storage, mock := newMockStorage(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	DebugCtlTaskFramework(true)
	assert.NoError(t, storage.PingContext(ctx))
	DebugCtlTaskFramework(false)
	assert.NoError(t, storage.PingContext(ctx))
	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestAsyncTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)
	mock.ExpectExec(insertAsyncTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("a", 0, []byte(nil), "{}", "", 0, "", 0, 0, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddAsyncTask(context.Background(), newTaskFromMetadata(task.TaskMetadata{ID: "a"}))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	mock.ExpectQuery(selectAsyncTask + " AND task_id=1 order by task_id").
		WillReturnRows(sqlmock.NewRows(asyncRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "", 0, "", 0, 0, 0, "", 0, 0))
	asyncTask, err := storage.QueryAsyncTask(context.Background(), WithTaskIDCond(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(asyncTask))
	assert.Equal(t, "a", asyncTask[0].Metadata.ID)

	mock.ExpectBegin()
	mock.ExpectExec(updateAsyncTask+" AND task_epoch=0").
		WithArgs(0, []byte(nil), "{}", "", 0, "c1", 0, 0, 0, "", 0, 0, 1).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	asyncTask[0].TaskRunner = "c1"
	affected, err = storage.UpdateAsyncTask(context.Background(), []task.AsyncTask{asyncTask[0]}, WithTaskEpochCond(EQ, 0))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestCronTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)
	mock.ExpectExec(insertCronTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("a", 0, []byte(nil), "{}", "mock_cron_expr", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddCronTask(context.Background(), newTestCronTask("a", "mock_cron_expr"))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	mock.ExpectQuery(selectCronTask + " AND cron_task_id=1").
		WillReturnRows(sqlmock.NewRows(cronRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "mock_cron_expr", 0, 0, 0, 0))
	cronTask, err := storage.QueryCronTask(context.Background(), WithCronTaskId(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cronTask))
	assert.Equal(t, "a", cronTask[0].Metadata.ID)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func newMockStorage(t *testing.T) (*mysqlTaskStorage, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	return &mysqlTaskStorage{
		db: db,
	}, mock
}

func newCdcInfo(t *testing.T) task.DaemonTask {
	details := &task.Details{
		AccountID: catalog.System_Account,
		Account:   "sys",
		Username:  "root",
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: "task1",
				TaskId:   "taskID-1",
				Accounts: []*task.Account{
					{
						Id:   uint64(catalog.System_Account),
						Name: "sys",
					},
				},
			},
		},
	}

	mdata := task.TaskMetadata{
		ID:       "taskID-1",
		Executor: task.TaskCode_InitCdc,
		Options: task.TaskOptions{
			MaxRetryTimes: 3,
			RetryInterval: 10,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}

	dt := task.DaemonTask{
		Metadata:   mdata,
		TaskType:   details.Type(),
		TaskStatus: task.TaskStatus_Created,
		Details:    details,
		CreateAt:   time.Now(),
		UpdateAt:   time.Now(),
	}
	return dt
}

func newDaemonTaskRows(t *testing.T, dt task.DaemonTask) *sqlmock.Rows {
	dbytes, err := dt.Details.Marshal()
	assert.NoError(t, err)

	return sqlmock.NewRows(daemonHeads).AddRow(
		0,
		dt.Metadata.ID,
		dt.Metadata.Executor,
		dt.Metadata.Context,
		json.MustMarshal(dt.Metadata.Options),
		catalog.System_Account,
		"sys",
		dt.TaskType,
		dt.TaskRunner,
		dt.TaskStatus,
		dt.LastHeartbeat,
		dt.CreateAt,
		dt.UpdateAt,
		dt.EndAt,
		dt.LastRun,
		dbytes,
	)
}

func newInsertDaemonTaskExpect(t *testing.T, mock sqlmock.Sqlmock) {
	mock.ExpectExec(insertDaemonTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func TestAddCdcTask(t *testing.T) {
	storage, mock := newMockStorage(t)

	dt := newCdcInfo(t)

	mock.ExpectBegin()
	newInsertDaemonTaskExpect(t, mock)

	callback := func(context.Context, SqlExecutor) (int, error) {
		return 1, nil
	}
	mock.ExpectCommit()
	affected, err := storage.AddCDCTask(
		context.Background(),
		dt,
		callback,
	)
	assert.NoError(t, err)
	assert.Greater(t, affected, 0)

	mock.ExpectBegin()

	mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(
		newDaemonTaskRows(t, dt),
	)

	mock.ExpectExec(updateDaemonTask).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(0, 1))

	callback2 := func(ctx context.Context, ts task.TaskStatus, keyMap map[CDCTaskKey]struct{}, se SqlExecutor) (int, error) {
		keyMap[CDCTaskKey{
			AccountId: uint64(catalog.System_Account),
			TaskId:    dt.Metadata.ID,
		}] = struct{}{}
		return 1, nil
	}

	affected, err = storage.UpdateCDCTask(
		context.Background(),
		task.TaskStatus_Canceled,
		callback2,
	)
	assert.NoError(t, err)
	assert.Greater(t, affected, 0)

	mock.ExpectBegin()

	dt.TaskStatus = task.TaskStatus_Paused
	mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(
		newDaemonTaskRows(t, dt),
	)

	_, err = storage.UpdateCDCTask(
		context.Background(),
		task.TaskStatus_PauseRequested,
		callback2,
	)
	assert.Error(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func Test_AddDaemonTask(t *testing.T) {
	storage, mock := newMockStorage(t)

	details := &task.Details{
		AccountID: catalog.System_Account,
		Account:   "sys",
		Username:  "root",
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: "task1",
				TaskId:   "taskID-1",
				Accounts: []*task.Account{
					{
						Id:   uint64(catalog.System_Account),
						Name: "sys",
					},
				},
			},
		},
	}

	mdata := task.TaskMetadata{
		ID:       "taskID-1",
		Executor: task.TaskCode_InitCdc,
		Options: task.TaskOptions{
			MaxRetryTimes: 3,
			RetryInterval: 10,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}

	dt := task.DaemonTask{
		Metadata:   mdata,
		TaskType:   details.Type(),
		TaskStatus: task.TaskStatus_Created,
		Details:    details,
		CreateAt:   time.Now(),
		UpdateAt:   time.Now(),
	}

	newInsertDaemonTaskExpect(t, mock)

	cnt, err := storage.AddDaemonTask(context.Background(), dt)
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0)

	mock.ExpectBegin()
	mock.ExpectExec(updateDaemonTask).WithArgs(
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	cnt, err = storage.UpdateDaemonTask(context.Background(), []task.DaemonTask{dt})
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0)

	mock.ExpectExec(deleteDaemonTask).WillReturnResult(sqlmock.NewResult(0, 1))

	cnt, err = storage.DeleteDaemonTask(context.Background())
	assert.NoError(t, err)
	assert.Greater(t, cnt, 0)

	mock.ExpectClose()
	_ = storage.Close()
}

func Test_labels(t *testing.T) {
	storage, mock := newMockStorage(t)

	dt := newCdcInfo(t)

	mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(newDaemonTaskRows(t, dt))

	labels := NewCnLabels("xxx")
	labels.Add("xxx", []string{"sys"})
	fmt.Println(labels.String())
	cond := WithLabels(IN, labels)

	tks, err := storage.QueryDaemonTask(context.Background(), cond)
	assert.NoError(t, err)
	assert.Greater(t, len(tks), 0)

	mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(newDaemonTaskRows(t, dt))

	labels2 := NewCnLabels("xxx")
	labels2.Add("yyy", []string{"sys"})
	fmt.Println(labels2.String())
	cond2 := WithLabels(IN, labels2)

	tks, err = storage.QueryDaemonTask(context.Background(), cond2)
	assert.NoError(t, err)
	assert.Equal(t, len(tks), 0)

	_ = mock.ExpectClose()
	_ = storage.Close()
}

func TestUpdateAsyncTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)
	mock.ExpectExec(insertAsyncTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("a", 0, []byte(nil), "{}", "", 0, "", 0, 0, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddAsyncTask(context.Background(), newTaskFromMetadata(task.TaskMetadata{ID: "a"}))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	mock.ExpectQuery(selectAsyncTask + " AND task_id=1 order by task_id").
		WillReturnRows(sqlmock.NewRows(asyncRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "", 0, "", 0, 0, 0, "", 0, 0))
	asyncTask, err := storage.QueryAsyncTask(context.Background(), WithTaskIDCond(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(asyncTask))
	assert.Equal(t, "a", asyncTask[0].Metadata.ID)

	mock.ExpectBegin()
	updateSql := "update sys_async_task set error_msg=NULL where task_id=1"
	mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	asyncTask[0].TaskRunner = "c1"
	tx, err := storage.db.Begin()
	assert.NoError(t, err)
	_, err = tx.ExecContext(context.Background(), updateSql)
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())

	mock.ExpectQuery(selectAsyncTask + " AND task_id=1 order by task_id").
		WillReturnRows(sqlmock.NewRows(asyncRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "", 0, "", 0, 0, 0, nil, 0, 0))
	_, err = storage.QueryAsyncTask(context.Background(), WithTaskIDCond(EQ, 1))
	assert.NoError(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestDaemonTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)
	mock.ExpectExec(insertDaemonTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("-", 4, []byte(nil), "{}", 0, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddDaemonTask(context.Background(), newDaemonTaskForTest(1, task.TaskStatus_Created, ""))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	mock.ExpectQuery(selectDaemonTask + " AND task_id=1 order by task_id").
		WillReturnRows(sqlmock.NewRows(daemonRows).
			AddRow(1, "a", 0, []byte(nil), "{}", 0, 0, "", 0, 0, time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Time{}, 0))

	daemonTask, err := storage.QueryDaemonTask(context.Background(), WithTaskIDCond(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(daemonTask))
	assert.Equal(t, "a", daemonTask[0].Metadata.ID)

	mock.ExpectBegin()
	mock.ExpectExec(updateDaemonTask).
		WithArgs(0, []byte(nil), "{}", "Unknown", 0, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	update, err := storage.UpdateDaemonTask(context.Background(), []task.DaemonTask{{
		ID: 0, Metadata: task.TaskMetadata{ID: "test"}, Details: &task.Details{},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 1, update)

	mock.ExpectBegin()
	mock.ExpectExec(heartbeatDaemonTask).WithArgs(time.Time{}, 0).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	heartbeat, err := storage.HeartbeatDaemonTask(context.Background(), []task.DaemonTask{{
		ID: 0, Metadata: task.TaskMetadata{ID: "test"},
	}})
	assert.NoError(t, err)
	assert.Equal(t, 1, heartbeat)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestNewMysqlTaskStorage(t *testing.T) {
	store, err := newMysqlTaskStorage("root:111@tcp(127.0.0.1:3306)/mo_task")
	require.NoError(t, err)
	require.NotNil(t, store)
	require.NoError(t, store.Close())
}

func TestDeleteAsyncTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)
	expectedSQL := deleteAsyncTask + buildWhereClause(newConditions(WithTaskStatusCond(task.TaskStatus_Completed)))
	mock.ExpectExec(expectedSQL).WillReturnResult(sqlmock.NewResult(0, 2))

	affected, err := storage.DeleteAsyncTask(context.Background(), WithTaskStatusCond(task.TaskStatus_Completed))
	require.NoError(t, err)
	require.Equal(t, 2, affected)

	mock.ExpectExec(expectedSQL).WillReturnError(errors.New("delete failed"))
	_, err = storage.DeleteAsyncTask(context.Background(), WithTaskStatusCond(task.TaskStatus_Completed))
	require.Error(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestTaskExistsAndGetTriggerTimes(t *testing.T) {
	storage, mock := newMockStorage(t)

	mock.ExpectQuery(countTaskId).WithArgs("task-a").
		WillReturnRows(sqlmock.NewRows([]string{"count(task_metadata_id)"}).AddRow(1))
	ok, err := storage.taskExists(context.Background(), "task-a")
	require.NoError(t, err)
	require.True(t, ok)

	mock.ExpectQuery(getTriggerTimes).WithArgs("cron-a").
		WillReturnRows(sqlmock.NewRows([]string{"trigger_times"}).AddRow(uint64(9)))
	trigger, err := storage.getTriggerTimes(context.Background(), "cron-a")
	require.NoError(t, err)
	require.Equal(t, uint64(9), trigger)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestUpdateCronTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t)

	cronTask := newTestCronTask("cron-task-1", "*/5 * * * * *")
	cronTask.ID = 11
	cronTask.TriggerTimes = 2
	cronTask.Metadata.Executor = task.TaskCode_TestOnly
	cronTask.Metadata.Context = []byte("cron-context")
	cronTask.CreateAt = 100
	cronTask.UpdateAt = 200
	cronTask.NextTime = 300

	asyncTask := newTestAsyncTask("cron-task-1")
	asyncTask.Metadata.Executor = task.TaskCode_TestOnly
	asyncTask.Metadata.Context = []byte("async-context")
	asyncTask.Status = task.TaskStatus_Running
	asyncTask.TaskRunner = "runner-1"
	asyncTask.Epoch = 1
	asyncTask.LastHeartbeat = 10
	asyncTask.CreateAt = 11
	asyncTask.CompletedAt = 12

	mock.ExpectQuery(countTaskId).WithArgs(asyncTask.Metadata.ID).
		WillReturnRows(sqlmock.NewRows([]string{"count(task_metadata_id)"}).AddRow(0))
	mock.ExpectQuery(getTriggerTimes).WithArgs(cronTask.Metadata.ID).
		WillReturnRows(sqlmock.NewRows([]string{"trigger_times"}).AddRow(uint64(1)))
	mock.ExpectBegin()
	mock.ExpectExec(insertAsyncTask+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs(
			asyncTask.Metadata.ID,
			asyncTask.Metadata.Executor,
			asyncTask.Metadata.Context,
			sqlmock.AnyArg(),
			asyncTask.ParentTaskID,
			asyncTask.Status,
			asyncTask.TaskRunner,
			asyncTask.Epoch,
			asyncTask.LastHeartbeat,
			asyncTask.CreateAt,
			asyncTask.CompletedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(updateCronTask).
		WithArgs(
			cronTask.Metadata.Executor,
			cronTask.Metadata.Context,
			sqlmock.AnyArg(),
			cronTask.CronExpr,
			cronTask.NextTime,
			cronTask.TriggerTimes,
			cronTask.CreateAt,
			cronTask.UpdateAt,
			cronTask.ID).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	affected, err := storage.UpdateCronTask(context.Background(), cronTask, asyncTask)
	require.NoError(t, err)
	require.Equal(t, 2, affected)

	// task exists branch
	mock.ExpectQuery(countTaskId).WithArgs(asyncTask.Metadata.ID).
		WillReturnRows(sqlmock.NewRows([]string{"count(task_metadata_id)"}).AddRow(1))
	affected, err = storage.UpdateCronTask(context.Background(), cronTask, asyncTask)
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	// trigger times mismatch branch
	mock.ExpectQuery(countTaskId).WithArgs(asyncTask.Metadata.ID).
		WillReturnRows(sqlmock.NewRows([]string{"count(task_metadata_id)"}).AddRow(0))
	mock.ExpectQuery(getTriggerTimes).WithArgs(cronTask.Metadata.ID).
		WillReturnRows(sqlmock.NewRows([]string{"trigger_times"}).AddRow(uint64(99)))
	_, err = storage.UpdateCronTask(context.Background(), cronTask, asyncTask)
	require.Error(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestRemoveDuplicateTasks(t *testing.T) {
	asyncTasks := []task.AsyncTask{
		{Metadata: task.TaskMetadata{ID: "a"}},
		{Metadata: task.TaskMetadata{ID: "b"}},
	}
	cronTasks := []task.CronTask{
		{Metadata: task.TaskMetadata{ID: "a"}},
		{Metadata: task.TaskMetadata{ID: "b"}},
	}
	daemonTasks := []task.DaemonTask{
		{Metadata: task.TaskMetadata{ID: "a"}},
		{Metadata: task.TaskMetadata{ID: "b"}},
	}

	dupErr := &mysqlDriver.MySQLError{
		Number:  moerr.ER_DUP_ENTRY,
		Message: "Duplicate entry 'a' for key",
	}
	otherErr := errors.New("not mysql err")

	remainingAsync, err := removeDuplicateAsyncTasks(dupErr, asyncTasks)
	require.NoError(t, err)
	require.Len(t, remainingAsync, 1)
	require.Equal(t, "b", remainingAsync[0].Metadata.ID)

	remainingCron, err := removeDuplicateCronTasks(dupErr, cronTasks)
	require.NoError(t, err)
	require.Len(t, remainingCron, 1)
	require.Equal(t, "b", remainingCron[0].Metadata.ID)

	remainingDaemon, err := removeDuplicateDaemonTasks(dupErr, daemonTasks)
	require.NoError(t, err)
	require.Len(t, remainingDaemon, 1)
	require.Equal(t, "b", remainingDaemon[0].Metadata.ID)

	_, err = removeDuplicateAsyncTasks(otherErr, asyncTasks)
	require.Error(t, err)
	_, err = removeDuplicateCronTasks(otherErr, cronTasks)
	require.Error(t, err)
	_, err = removeDuplicateDaemonTasks(otherErr, daemonTasks)
	require.Error(t, err)
}

func TestAddAsyncTaskWithDuplicateEntry(t *testing.T) {
	storage, mock := newMockStorage(t)

	tasks := []task.AsyncTask{
		newTestAsyncTask("dup-a"),
		newTestAsyncTask("dup-b"),
	}
	dupErr := &mysqlDriver.MySQLError{
		Number:  moerr.ER_DUP_ENTRY,
		Message: "Duplicate entry 'dup-a' for key",
	}
	mock.ExpectExec(insertAsyncTask + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WillReturnError(dupErr)
	mock.ExpectExec(insertAsyncTask + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddAsyncTask(context.Background(), tasks...)
	require.NoError(t, err)
	require.Equal(t, 1, affected)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestAddCronTaskWithDuplicateEntry(t *testing.T) {
	storage, mock := newMockStorage(t)

	c1 := newTestCronTask("cron-a", "* * * * * *")
	c2 := newTestCronTask("cron-b", "* * * * * *")
	dupErr := &mysqlDriver.MySQLError{
		Number:  moerr.ER_DUP_ENTRY,
		Message: "Duplicate entry 'cron-a' for key",
	}
	mock.ExpectExec(insertCronTask + "(?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WillReturnError(dupErr)
	mock.ExpectExec(insertCronTask + "(?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddCronTask(context.Background(), c1, c2)
	require.NoError(t, err)
	require.Equal(t, 1, affected)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func TestMysqlTaskStorageDisabledBranch(t *testing.T) {
	storage, mock := newMockStorage(t)
	DebugCtlTaskFramework(true)
	defer DebugCtlTaskFramework(false)

	affected, err := storage.AddAsyncTask(context.Background(), newTestAsyncTask("x"))
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	affected, err = storage.DeleteAsyncTask(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	affected, err = storage.AddCronTask(context.Background(), newTestCronTask("c", "* * * * * *"))
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	affected, err = storage.AddDaemonTask(context.Background(), newDaemonTaskForTest(1, task.TaskStatus_Created, ""))
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	_, err = storage.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	_, err = storage.QueryCronTask(context.Background())
	require.NoError(t, err)
	_, err = storage.QueryDaemonTask(context.Background())
	require.NoError(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

type mockRowsAffectedResult struct {
	rows int64
	err  error
}

func (r mockRowsAffectedResult) LastInsertId() (int64, error) { return 0, nil }

func (r mockRowsAffectedResult) RowsAffected() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.rows, nil
}

type mockSqlExecutor struct {
	execFn func(context.Context, string, ...interface{}) (sql.Result, error)
}

func (m *mockSqlExecutor) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (m *mockSqlExecutor) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockSqlExecutor) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not implemented")
}

func TestBuildLimitAndOrderByClause(t *testing.T) {
	c := newConditions(WithLimitCond(5), WithTaskIDDesc())
	require.Equal(t, " limit 5", buildLimitClause(c))
	require.Equal(t, " order by task_id desc", buildOrderByClause(c))

	c = newConditions(WithTaskStatusCond(task.TaskStatus_Created))
	require.Equal(t, "", buildLimitClause(c))
	require.Equal(t, " order by task_id", buildOrderByClause(c))
}

func TestRunAddDaemonTaskBranches(t *testing.T) {
	m := &mysqlTaskStorage{}
	ctx := context.Background()

	t.Run("empty tasks", func(t *testing.T) {
		n, err := m.RunAddDaemonTask(ctx, &mockSqlExecutor{})
		require.NoError(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("duplicate then retry", func(t *testing.T) {
		d1 := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
		d1.Metadata.ID = "dup-daemon"
		d2 := newDaemonTaskForTest(2, task.TaskStatus_Created, "")
		d2.Metadata.ID = "keep-daemon"

		calls := 0
		exec := &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				calls++
				if calls == 1 {
					return nil, &mysqlDriver.MySQLError{
						Number:  moerr.ER_DUP_ENTRY,
						Message: "Duplicate entry 'dup-daemon' for key",
					}
				}
				return mockRowsAffectedResult{rows: 1}, nil
			},
		}

		n, err := m.RunAddDaemonTask(ctx, exec, d1, d2)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, 2, calls)
	})

	t.Run("non-duplicate error", func(t *testing.T) {
		d := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
		d.Metadata.ID = "err-daemon"
		_, err := m.RunAddDaemonTask(ctx, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return nil, errors.New("exec failed")
			},
		}, d)
		require.Error(t, err)
	})

	t.Run("rows affected error", func(t *testing.T) {
		d := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
		d.Metadata.ID = "rows-daemon"
		_, err := m.RunAddDaemonTask(ctx, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return mockRowsAffectedResult{err: errors.New("rows failed")}, nil
			},
		}, d)
		require.Error(t, err)
	})
}

func TestUpdateDaemonTaskBranchesInSqlMock(t *testing.T) {
	ctx := context.Background()
	d := newDaemonTaskForTest(1, task.TaskStatus_Running, "r1")
	d.Metadata.ID = "update-daemon"

	t.Run("empty tasks", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		n, err := storage.UpdateDaemonTask(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("begin tx error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		_, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("rollback join error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectExec(updateDaemonTask).WillReturnError(errors.New("exec failed"))
		mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))
		_, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exec failed")
		require.Contains(t, err.Error(), "rollback failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("commit error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectExec(updateDaemonTask).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		_, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		require.Contains(t, err.Error(), "commit failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})
}

func TestRunUpdateDaemonTaskBranches(t *testing.T) {
	m := &mysqlTaskStorage{}
	ctx := context.Background()

	d := newDaemonTaskForTest(1, task.TaskStatus_Running, "r1")
	d.Metadata.ID = "run-update-daemon"
	now := time.Now()
	d.LastHeartbeat = now
	d.UpdateAt = now
	d.EndAt = now
	d.LastRun = now

	t.Run("success", func(t *testing.T) {
		n, err := m.RunUpdateDaemonTask(ctx, []task.DaemonTask{d}, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return mockRowsAffectedResult{rows: 1}, nil
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})

	t.Run("exec error", func(t *testing.T) {
		_, err := m.RunUpdateDaemonTask(ctx, []task.DaemonTask{d}, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return nil, errors.New("exec failed")
			},
		})
		require.Error(t, err)
	})

	t.Run("rows affected error", func(t *testing.T) {
		_, err := m.RunUpdateDaemonTask(ctx, []task.DaemonTask{d}, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return mockRowsAffectedResult{err: errors.New("rows failed")}, nil
			},
		})
		require.Error(t, err)
	})
}

func TestRunDeleteDaemonTaskBranches(t *testing.T) {
	m := &mysqlTaskStorage{}
	ctx := context.Background()

	t.Run("exec error", func(t *testing.T) {
		_, err := m.RunDeleteDaemonTask(ctx, &mockSqlExecutor{
			execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
				return nil, errors.New("exec failed")
			},
		})
		require.Error(t, err)
	})

	t.Run("rows affected panic", func(t *testing.T) {
		require.Panics(t, func() {
			_, _ = m.RunDeleteDaemonTask(ctx, &mockSqlExecutor{
				execFn: func(context.Context, string, ...interface{}) (sql.Result, error) {
					return mockRowsAffectedResult{err: errors.New("rows failed")}, nil
				},
			})
		})
	})
}

func TestHeartbeatDaemonTaskBranchesInSqlMock(t *testing.T) {
	ctx := context.Background()
	d := newDaemonTaskForTest(1, task.TaskStatus_Running, "r1")
	d.Metadata.ID = "hb-daemon"

	t.Run("empty tasks", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		n, err := storage.HeartbeatDaemonTask(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("begin tx error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		_, err := storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("rollback join error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectExec(heartbeatDaemonTask).WillReturnError(errors.New("exec failed"))
		mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))
		_, err := storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exec failed")
		require.Contains(t, err.Error(), "rollback failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("rows affected error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectExec(heartbeatDaemonTask).WillReturnResult(sqlmock.NewErrorResult(errors.New("rows failed")))
		mock.ExpectRollback()
		_, err := storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		require.Contains(t, err.Error(), "rows failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("commit error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectExec(heartbeatDaemonTask).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		_, err := storage.HeartbeatDaemonTask(ctx, []task.DaemonTask{d})
		require.Error(t, err)
		require.Contains(t, err.Error(), "commit failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})
}

func TestAddCDCTaskBranchesInSqlMock(t *testing.T) {
	ctx := context.Background()
	d := newCdcInfo(t)

	t.Run("disabled", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		DebugCtlTaskFramework(true)
		n, err := storage.AddCDCTask(ctx, d, nil)
		DebugCtlTaskFramework(false)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("begin tx error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		_, err := storage.AddCDCTask(ctx, d, nil)
		require.Error(t, err)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("callback rollback join error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))
		_, err := storage.AddCDCTask(ctx, d, func(context.Context, SqlExecutor) (int, error) {
			return 0, errors.New("callback failed")
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "callback failed")
		require.Contains(t, err.Error(), "rollback failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("rows affected mismatch", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		newInsertDaemonTaskExpect(t, mock)
		mock.ExpectRollback()
		_, err := storage.AddCDCTask(ctx, d, func(context.Context, SqlExecutor) (int, error) {
			return 2, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "add cdc task status failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("commit error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		newInsertDaemonTaskExpect(t, mock)
		mock.ExpectCommit().WillReturnError(errors.New("commit failed"))
		_, err := storage.AddCDCTask(ctx, d, func(context.Context, SqlExecutor) (int, error) {
			return 1, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "commit failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})
}

func TestUpdateCDCTaskBranchesInSqlMock(t *testing.T) {
	ctx := context.Background()
	base := newCdcInfo(t)

	t.Run("nil collector", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		n, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, nil)
		require.NoError(t, err)
		require.Equal(t, 0, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("begin tx error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
		_, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error) {
			return 0, nil
		})
		require.Error(t, err)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("collector rollback join error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectRollback().WillReturnError(errors.New("rollback failed"))
		_, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error) {
			return 0, errors.New("collector failed")
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "collector failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("empty task key map", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectCommit()
		n, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, func(context.Context, task.TaskStatus, map[CDCTaskKey]struct{}, SqlExecutor) (int, error) {
			return 1, nil
		})
		require.NoError(t, err)
		require.Equal(t, 0, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("query daemon task error", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		mock.ExpectBegin()
		mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnError(errors.New("query failed"))
		mock.ExpectRollback()
		_, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, func(_ context.Context, _ task.TaskStatus, keyMap map[CDCTaskKey]struct{}, _ SqlExecutor) (int, error) {
			keyMap[CDCTaskKey{
				AccountId: uint64(catalog.System_Account),
				TaskId:    base.Metadata.ID,
			}] = struct{}{}
			return 1, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "query failed")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("non create-cdc details", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		notCdc := newDaemonTaskForTest(1, task.TaskStatus_Running, "r1")
		notCdc.Metadata.ID = "not-cdc"

		mock.ExpectBegin()
		mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(newDaemonTaskRows(t, notCdc))
		mock.ExpectRollback()
		_, err := storage.UpdateCDCTask(ctx, task.TaskStatus_Canceled, func(_ context.Context, _ task.TaskStatus, keyMap map[CDCTaskKey]struct{}, _ SqlExecutor) (int, error) {
			keyMap[CDCTaskKey{
				AccountId: uint64(catalog.System_Account),
				TaskId:    base.Metadata.ID,
			}] = struct{}{}
			return 1, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "Details not a CreateCdc task type")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("status mismatch", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		d := base
		d.ID = 1
		d.TaskStatus = task.TaskStatus_Paused

		mock.ExpectBegin()
		mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(newDaemonTaskRows(t, d))
		mock.ExpectRollback()
		_, err := storage.UpdateCDCTask(ctx, task.TaskStatus_PauseRequested, func(_ context.Context, _ task.TaskStatus, keyMap map[CDCTaskKey]struct{}, _ SqlExecutor) (int, error) {
			keyMap[CDCTaskKey{
				AccountId: uint64(catalog.System_Account),
				TaskId:    d.Metadata.ID,
			}] = struct{}{}
			return 1, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "status can not be change")
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})

	t.Run("success transition", func(t *testing.T) {
		storage, mock := newMockStorage(t)
		d := base
		d.ID = 1
		d.TaskStatus = task.TaskStatus_Running

		mock.ExpectBegin()
		mock.ExpectQuery(selectDaemonTask + " order by task_id").WillReturnRows(newDaemonTaskRows(t, d))
		mock.ExpectExec(updateDaemonTask).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		n, err := storage.UpdateCDCTask(ctx, task.TaskStatus_PauseRequested, func(_ context.Context, _ task.TaskStatus, keyMap map[CDCTaskKey]struct{}, _ SqlExecutor) (int, error) {
			keyMap[CDCTaskKey{
				AccountId: uint64(catalog.System_Account),
				TaskId:    d.Metadata.ID,
			}] = struct{}{}
			return 1, nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, n)
		mock.ExpectClose()
		require.NoError(t, storage.Close())
	})
}
