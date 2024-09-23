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
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
)

func TestAsyncTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t, "sqlmock")
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
	mock.ExpectPrepare(updateAsyncTask + " AND task_epoch=0")
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
	storage, mock := newMockStorage(t, "sqlmock")
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

func newMockStorage(t *testing.T, dsn string) (*mysqlTaskStorage, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.NewWithDSN(dsn, sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
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
	storage, mock := newMockStorage(t, "sqlmock")

	dt := newCdcInfo(t)

	mock.ExpectBegin()
	newInsertDaemonTaskExpect(t, mock)

	callback := func(context.Context, SqlExecutor) (int, error) {
		return 1, nil
	}

	affected, err := storage.AddCdcTask(
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

	mock.ExpectPrepare(updateDaemonTask)
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

	callback2 := func(ctx context.Context, ts task.TaskStatus, keyMap map[CdcTaskKey]struct{}, se SqlExecutor) (int, error) {
		keyMap[CdcTaskKey{
			AccountId: uint64(catalog.System_Account),
			TaskId:    dt.Metadata.ID,
		}] = struct{}{}
		return 1, nil
	}

	affected, err = storage.UpdateCdcTask(
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

	_, err = storage.UpdateCdcTask(
		context.Background(),
		task.TaskStatus_PauseRequested,
		callback2,
	)
	assert.Error(t, err)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func Test_AddDaemonTask(t *testing.T) {
	storage, mock := newMockStorage(t, "sqlmock")

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
	mock.ExpectPrepare(updateDaemonTask)
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
	storage, mock := newMockStorage(t, "sqlmock")

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
