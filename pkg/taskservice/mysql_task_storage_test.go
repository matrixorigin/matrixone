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
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"slices"
	"strings"
	"testing"
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

const (
	useDB    = "use mo_task"
	setTrace = "set session disable_txn_trace=1"
)

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

	expectUseDB = func(mock sqlmock.Sqlmock) {
		mock.ExpectExec(useDB).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(setTrace).WillReturnResult(sqlmock.NewResult(0, 1))
	}
)

func TestAsyncTaskInSqlMock(t *testing.T) {
	storage, mock := newMockStorage(t, "sqlmock", "mo_task")
	expectUseDB(mock)
	mock.ExpectPrepare(fmt.Sprintf(insertAsyncTask, storage.dbname) + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	mock.ExpectExec(fmt.Sprintf(insertAsyncTask, storage.dbname)+"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("a", 0, []byte(nil), "{}", "", 0, "", 0, 0, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddAsyncTask(context.Background(), newTaskFromMetadata(task.TaskMetadata{ID: "a"}))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	expectUseDB(mock)
	mock.ExpectQuery(fmt.Sprintf(selectAsyncTask, storage.dbname) + " AND task_id=1 order by task_id").
		WillReturnRows(sqlmock.NewRows(asyncRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "", 0, "", 0, 0, 0, "", 0, 0))
	asyncTask, err := storage.QueryAsyncTask(context.Background(), WithTaskIDCond(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(asyncTask))
	assert.Equal(t, "a", asyncTask[0].Metadata.ID)

	expectUseDB(mock)
	mock.ExpectBegin()
	mock.ExpectPrepare(fmt.Sprintf(updateAsyncTask, storage.dbname) + " AND task_epoch=0")
	mock.ExpectExec(fmt.Sprintf(updateAsyncTask, storage.dbname)+" AND task_epoch=0").
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
	storage, mock := newMockStorage(t, "sqlmock", "mo_task")
	expectUseDB(mock)
	mock.ExpectPrepare(fmt.Sprintf(insertCronTask, storage.dbname) + "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
	mock.ExpectExec(fmt.Sprintf(insertCronTask, storage.dbname)+"(?, ?, ?, ?, ?, ?, ?, ?, ?)").
		WithArgs("a", 0, []byte(nil), "{}", "mock_cron_expr", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	affected, err := storage.AddCronTask(context.Background(), newTestCronTask("a", "mock_cron_expr"))
	assert.NoError(t, err)
	assert.Equal(t, 1, affected)

	expectUseDB(mock)
	mock.ExpectQuery(fmt.Sprintf(selectCronTask, storage.dbname) + " AND cron_task_id=1").
		WillReturnRows(sqlmock.NewRows(cronRows).
			AddRow(1, "a", 0, []byte(nil), "{}", "mock_cron_expr", 0, 0, 0, 0))
	cronTask, err := storage.QueryCronTask(context.Background(), WithCronTaskId(EQ, 1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cronTask))
	assert.Equal(t, "a", cronTask[0].Metadata.ID)

	mock.ExpectClose()
	require.NoError(t, storage.Close())
}

func newMockStorage(t *testing.T, dsn, dbname string) (*mysqlTaskStorage, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.NewWithDSN(dsn, sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	return &mysqlTaskStorage{
		dsn:    dsn,
		db:     db,
		dbname: dbname,
	}, mock
}
