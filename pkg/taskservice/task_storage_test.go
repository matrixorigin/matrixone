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
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

var (
	storages = map[string]func(*testing.T) (TaskStorage, sqlmock.Sqlmock){
		"mem":     createMem,
		"refresh": createRefresh,
	}
)

func createMem(t *testing.T) (TaskStorage, sqlmock.Sqlmock) {
	return NewMemTaskStorage(), nil
}

func createRefresh(t *testing.T) (TaskStorage, sqlmock.Sqlmock) {
	client, mock1, _ := newTestClient(t)
	client.addressFunc = func() string { return "s1" }
	s := newRefreshableTaskStorage(
		runtime.DefaultRuntime(),
		client)
	s.(*refreshableTaskStorage).maybeRefresh(context.Background())
	return s, mock1
}

// TODO: move to cluster testing.
// func createMysql(t *testing.T) TaskStorage {
// 	storage, err := newMysqlTaskStorage("root:root@tcp(127.0.0.1:12345)/mo_task")
// 	require.NoError(t, err)
// 	return storage
// }

func TestAddAsyncTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v := newTestAsyncTask("t1")

			if mock != nil {
				b, err := json.Marshal(v.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, v.CreateAt, v.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, v.CreateAt, v.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 0))
				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt),
					)
				mock.ExpectClose()
			}

			mustAddTestAsyncTask(t, s, v)
			mustAddTestAsyncTask(t, s, v)
			require.Equal(t, 1, len(mustGetTestAsyncTask(t, s, 1)))
		})
	}
}

func TestUpdateAsyncTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v := newTestAsyncTask("t1")

			if mock != nil {
				b, err := json.Marshal(v.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, v.CreateAt, v.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt),
					)
				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=?").
					WithArgs(1, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
				mock.ExpectClose()
			}

			mustAddTestAsyncTask(t, s, v)

			tasks := mustGetTestAsyncTask(t, s, 1)
			tasks[0].Metadata.Executor = 1
			mustUpdateTestAsyncTask(t, s, tasks)
		})
	}
}

func TestUpdateAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v := newTestAsyncTask("t1")

			if mock != nil {
				b, err := json.Marshal(v.Metadata.Options)
				require.NoError(t, err)

				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, v.CreateAt, v.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt),
					)

				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=? AND task_runner='t2'").
					WithArgs(0, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()

				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=? AND task_runner='t1'").
					WithArgs(0, v.Metadata.Context, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()

				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=? AND task_id=2").
					WithArgs(0, []byte{1}, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()

				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=? AND task_id=1").
					WithArgs(0, []byte{1}, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()

				mock.ExpectBegin()
				mock.ExpectExec("update sys_async_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,task_parent_id=?,task_status=?,task_runner=?,task_epoch=?,last_heartbeat=?,result_code=?,error_msg=?,create_at=?,end_at=? where task_id=? AND task_id>0").
					WithArgs(0, []byte{1, 2}, string(b), v.ParentTaskID, v.Status, v.TaskRunner, v.Epoch, v.LastHeartbeat, 0, "", v.CreateAt, v.CompletedAt, 1).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()

				mock.ExpectClose()
			}

			mustAddTestAsyncTask(t, s, v)
			tasks := mustGetTestAsyncTask(t, s, 1)

			mustUpdateTestAsyncTask(t, s, tasks, WithTaskRunnerCond(EQ, "t2"))
			mustUpdateTestAsyncTask(t, s, tasks, WithTaskRunnerCond(EQ, "t1"))

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestAsyncTask(t, s, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestAsyncTask(t, s, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestAsyncTask(t, s, tasks, WithTaskIDCond(GT, 0))
		})
	}
}

func TestDeleteAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v1 := newTestAsyncTask("t1")
			v2 := newTestAsyncTask("t2")
			v3 := newTestAsyncTask("t3")

			if mock != nil {
				b1, err := json.Marshal(v1.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, v1.CreateAt, v1.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))

				b2, err := json.Marshal(v2.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b2), v2.ParentTaskID, v2.Status, v2.TaskRunner, v2.Epoch, v2.LastHeartbeat, v2.CreateAt, v2.CompletedAt).
					WillReturnResult(sqlmock.NewResult(2, 1))

				b3, err := json.Marshal(v3.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v3.Metadata.ID, v3.Metadata.Executor, v3.Metadata.Context, string(b3), v3.ParentTaskID, v3.Status, v3.TaskRunner, v3.Epoch, v3.LastHeartbeat, v3.CreateAt, v3.CompletedAt).
					WillReturnResult(sqlmock.NewResult(3, 1))

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b2), v2.ParentTaskID, v2.Status, v2.TaskRunner, v2.Epoch, v2.LastHeartbeat, 0, "", v2.CreateAt, v2.CompletedAt).
							AddRow(1, v3.Metadata.ID, v3.Metadata.Executor, v3.Metadata.Context, string(b3), v3.ParentTaskID, v3.Status, v3.TaskRunner, v3.Epoch, v3.LastHeartbeat, 0, "", v3.CreateAt, v3.CompletedAt),
					)

				mock.ExpectExec("delete from sys_async_task where 1=1 AND task_runner='t4'").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("delete from sys_async_task where 1=1 AND task_runner='t1'").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("delete from sys_async_task where 1=1 AND task_id=2").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("delete from sys_async_task where 1=1 AND task_id>1").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}),
					)

				mock.ExpectClose()
			}

			mustAddTestAsyncTask(t, s, v1)
			mustAddTestAsyncTask(t, s, v2)
			mustAddTestAsyncTask(t, s, v3)
			tasks := mustGetTestAsyncTask(t, s, 3)

			mustDeleteTestAsyncTask(t, s, WithTaskRunnerCond(EQ, "t4"))
			mustDeleteTestAsyncTask(t, s, WithTaskRunnerCond(EQ, "t1"))

			mustDeleteTestAsyncTask(t, s, WithTaskIDCond(EQ, tasks[len(tasks)-1].ID+1))
			mustDeleteTestAsyncTask(t, s, WithTaskIDCond(GT, tasks[0].ID))

			mustGetTestAsyncTask(t, s, 0)
		})
	}
}

func TestQueryAsyncTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v1 := newTestAsyncTask("t1")
			v2 := newTestAsyncTask("t2")
			v3 := newTestAsyncTask("t3")

			if mock != nil {
				b1, err := json.Marshal(v1.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, v1.CreateAt, v1.CompletedAt).
					WillReturnResult(sqlmock.NewResult(1, 1))

				b2, err := json.Marshal(v2.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b2), v2.ParentTaskID, v2.Status, v2.TaskRunner, v2.Epoch, v2.LastHeartbeat, v2.CreateAt, v2.CompletedAt).
					WillReturnResult(sqlmock.NewResult(2, 1))

				b3, err := json.Marshal(v3.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v3.Metadata.ID, v3.Metadata.Executor, v3.Metadata.Context, string(b3), v3.ParentTaskID, v3.Status, v3.TaskRunner, v3.Epoch, v3.LastHeartbeat, v3.CreateAt, v3.CompletedAt).
					WillReturnResult(sqlmock.NewResult(3, 1))

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b2), v2.ParentTaskID, v2.Status, v2.TaskRunner, v2.Epoch, v2.LastHeartbeat, 0, "", v2.CreateAt, v2.CompletedAt).
							AddRow(1, v3.Metadata.ID, v3.Metadata.Executor, v3.Metadata.Context, string(b3), v3.ParentTaskID, v3.Status, v3.TaskRunner, v3.Epoch, v3.LastHeartbeat, 0, "", v3.CreateAt, v3.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 order by task_id limit 1").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_runner='t1' order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id>1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id>=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id<=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id<1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id>1 order by task_id limit 1").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectQuery(
					"select task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,result_code,error_msg,create_at,end_at from sys_async_task where 1=1 AND task_id=1 order by task_id").
					WillReturnRows(
						sqlmock.NewRows([]string{"task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "task_parent_id", "task_status", "task_runner", "task_epoch", "last_heartbeat", "result_code", "error_msg", "create_at", "end_at"}).
							AddRow(1, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.ParentTaskID, v1.Status, v1.TaskRunner, v1.Epoch, v1.LastHeartbeat, 0, "", v1.CreateAt, v1.CompletedAt),
					)

				mock.ExpectClose()
			}

			mustAddTestAsyncTask(t, s, v1)
			mustAddTestAsyncTask(t, s, v2)
			mustAddTestAsyncTask(t, s, v3)
			tasks := mustGetTestAsyncTask(t, s, 3)

			mustGetTestAsyncTask(t, s, 1, WithLimitCond(1))
			mustGetTestAsyncTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestAsyncTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestAsyncTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestAsyncTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestAsyncTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
		})
	}
}

func TestAddAndQueryCronTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v1 := newTestCronTask("t1", "cron1")
			v2 := newTestCronTask("t2", "cron2")

			if mock != nil {
				b1, err := json.Marshal(v1.Metadata.Options)
				require.NoError(t, err)

				mock.ExpectQuery(
					"select cron_task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at from sys_cron_task where 1=1").
					WillReturnRows(
						sqlmock.NewRows([]string{"cron_task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "cron_expr", "next_time", "trigger_times", "create_at", "update_at"}),
						//AddRow(v1.ID, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt),
					)

				mock.ExpectExec("insert into sys_cron_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt,
						v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b1), v2.CronExpr, v2.NextTime, v2.TriggerTimes, v2.CreateAt, v2.UpdateAt).
					WillReturnResult(sqlmock.NewResult(2, 2))

				mock.ExpectQuery(
					"select cron_task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at from sys_cron_task where 1=1").
					WillReturnRows(
						sqlmock.NewRows([]string{"cron_task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "cron_expr", "next_time", "trigger_times", "create_at", "update_at"}).
							AddRow(v1.ID, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt).
							AddRow(v2.ID, v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b1), v2.CronExpr, v2.NextTime, v2.TriggerTimes, v2.CreateAt, v2.UpdateAt),
					)

				mock.ExpectExec("insert into sys_cron_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?),(?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt,
						v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b1), v2.CronExpr, v2.NextTime, v2.TriggerTimes, v2.CreateAt, v2.UpdateAt).
					WillReturnResult(sqlmock.NewResult(2, 0))

				mock.ExpectQuery(
					"select cron_task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at from sys_cron_task where 1=1").
					WillReturnRows(
						sqlmock.NewRows([]string{"cron_task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "cron_expr", "next_time", "trigger_times", "create_at", "update_at"}).
							AddRow(v1.ID, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt).
							AddRow(v2.ID, v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b1), v2.CronExpr, v2.NextTime, v2.TriggerTimes, v2.CreateAt, v2.UpdateAt),
					)

				mock.ExpectClose()
			}

			mustQueryTestCronTask(t, s, 0)

			mustAddTestCronTask(t, s, 2, v1, v2)
			mustQueryTestCronTask(t, s, 2)

			mustAddTestCronTask(t, s, 0, v1, v2)
			mustQueryTestCronTask(t, s, 2)
		})
	}
}

func TestUpdateCronTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v1 := newTestCronTask("t1", "cron1")
			v2 := newTestAsyncTask("t1-cron-1")
			v3 := newTestAsyncTask("t1-cron-2")

			if mock != nil {
				mock.ExpectQuery(
					"select count(task_metadata_id) from sys_async_task where task_metadata_id=?").
					WithArgs(v2.Metadata.ID).
					WillReturnRows(sqlmock.NewRows([]string{"count(task_metadata_id)"}).AddRow(1))

				b1, err := json.Marshal(v1.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec("insert into sys_cron_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt).
					WillReturnResult(sqlmock.NewResult(1, 1))

				b2, err := json.Marshal(v2.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v2.Metadata.ID, v2.Metadata.Executor, v2.Metadata.Context, string(b2), v2.ParentTaskID, v2.Status, v2.TaskRunner, v2.Epoch, v2.LastHeartbeat, v2.CreateAt, v2.CompletedAt).
					WillReturnResult(sqlmock.NewResult(2, 1))

				mock.ExpectQuery(
					"select cron_task_id,task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,cron_expr,next_time,trigger_times,create_at,update_at from sys_cron_task where 1=1").
					WillReturnRows(
						sqlmock.NewRows([]string{"cron_task_id", "task_metadata_id", "task_metadata_executor", "task_metadata_context", "task_metadata_option", "cron_expr", "next_time", "trigger_times", "create_at", "update_at"}).
							AddRow(v1.ID, v1.Metadata.ID, v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, v1.TriggerTimes, v1.CreateAt, v1.UpdateAt),
					)

				mock.ExpectQuery(
					"select count(task_metadata_id) from sys_async_task where task_metadata_id=?").
					WithArgs(v2.Metadata.ID).
					WillReturnRows(
						sqlmock.NewRows([]string{"count(task_metadata_id)"}).
							AddRow(1),
					)

				mock.ExpectQuery(
					"select count(task_metadata_id) from sys_async_task where task_metadata_id=?").
					WithArgs(v3.Metadata.ID).
					WillReturnRows(
						sqlmock.NewRows([]string{"count(task_metadata_id)"}).
							AddRow(1),
					)

				mock.ExpectQuery(
					"select count(task_metadata_id) from sys_async_task where task_metadata_id=?").
					WithArgs(v3.Metadata.ID).
					WillReturnRows(
						sqlmock.NewRows([]string{"count(task_metadata_id)"}).
							AddRow(0),
					)

				mock.ExpectBegin()
				b3, err := json.Marshal(v3.Metadata.Options)
				require.NoError(t, err)
				mock.ExpectExec(
					"insert into sys_async_task(task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,task_parent_id,task_status,task_runner,task_epoch,last_heartbeat,create_at,end_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(v3.Metadata.ID, v3.Metadata.Executor, v3.Metadata.Context, string(b3), v3.ParentTaskID, v3.Status, v3.TaskRunner, v3.Epoch, v3.LastHeartbeat, v3.CreateAt, v3.CompletedAt).
					WillReturnResult(sqlmock.NewResult(2, 1))

				mock.ExpectExec(
					"update sys_cron_task set task_metadata_executor=?,task_metadata_context=?,task_metadata_option=?,cron_expr=?,next_time=?,trigger_times=?,create_at=?,update_at=? where cron_task_id=?").
					WithArgs(v1.Metadata.Executor, v1.Metadata.Context, string(b1), v1.CronExpr, v1.NextTime, 1, v1.CreateAt, v1.UpdateAt, v1.ID).
					WillReturnResult(sqlmock.NewResult(2, 1))

				mock.ExpectCommit()
				mock.ExpectClose()
			}

			ctx := context.Background()
			n, err := s.UpdateCronTask(ctx, v1, v2)
			require.NoError(t, err)
			require.Equal(t, 0, n)

			mustAddTestCronTask(t, s, 1, v1)
			mustAddTestAsyncTask(t, s, v2)

			v1 = mustQueryTestCronTask(t, s, 1)[0]

			n, err = s.UpdateCronTask(ctx, v1, v2)
			require.NoError(t, err)
			require.Equal(t, 0, n)

			n, err = s.UpdateCronTask(ctx, v1, v3)
			require.NoError(t, err)
			require.Equal(t, 0, n)

			v1.TriggerTimes++
			n, err = s.UpdateCronTask(ctx, v1, v3)
			require.NoError(t, err)
			require.Equal(t, 2, n)
		})
	}
}

func TestAddDaemonTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, mock := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v := newTestDaemonTask(1, "t1")

			if mock != nil {
				b, err := json.Marshal(v.Metadata.Options)
				require.NoError(t, err)

				mock.ExpectExec("insert into sys_daemon_task (task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,account_id,account,task_type,task_status,create_at,update_at,details) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectExec("insert into sys_daemon_task (task_metadata_id,task_metadata_executor,task_metadata_context,task_metadata_option,account_id,account,task_type,task_status,create_at,update_at,details) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 0))

				mock.ExpectQuery("select task_id, task_metadata_id, task_metadata_executor, task_metadata_context, task_metadata_option, account_id, account, task_type, task_runner, task_status, last_heartbeat, create_at, update_at, end_at, last_run, details from sys_daemon_task where 1=1 order by task_id").
					WillReturnRows(sqlmock.NewRows([]string{"task_id", " task_metadata_id", " task_metadata_executor", " task_metadata_context", " task_metadata_option", " account_id", " account", " task_type", " task_runner", " task_status", " last_heartbeat", " create_at", " update_at", " end_at", " last_run", " details"}).
						AddRow(v.ID, v.Metadata.ID, v.Metadata.Executor, v.Metadata.Context, string(b), v.AccountID, v.Account, v.TaskType, v.TaskRunner, v.TaskStatus, v.LastHeartbeat, v.CreateAt, v.UpdateAt, v.EndAt, v.LastRun, v.Details))

				mock.ExpectClose()
			}

			mustAddTestDaemonTask(t, s, 1, v)
			mustAddTestDaemonTask(t, s, 0, v)
			require.Equal(t, 1, len(mustGetTestDaemonTask(t, s, 1)))
		})
	}
}

func TestUpdateDaemonTask(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, _ := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			v := newTestDaemonTask(1, "t1")
			mustAddTestDaemonTask(t, s, 1, v)

			tasks := mustGetTestDaemonTask(t, s, 1)
			tasks[0].Metadata.Executor = 1
			mustUpdateTestDaemonTask(t, s, 1, tasks)
		})
	}
}

func TestUpdateDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, _ := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			tasks := mustGetTestDaemonTask(t, s, 1)

			mustUpdateTestDaemonTask(t, s, 0, tasks, WithTaskRunnerCond(EQ, "t2"))
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskRunnerCond(EQ, "t1"))

			tasks[0].Metadata.Context = []byte{1}
			mustUpdateTestDaemonTask(t, s, 0, tasks, WithTaskIDCond(EQ, tasks[0].ID+1))
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(EQ, tasks[0].ID))
			tasks[0].Metadata.Context = []byte{1, 2}
			mustUpdateTestDaemonTask(t, s, 1, tasks, WithTaskIDCond(GT, 0))
		})
	}
}

func TestDeleteDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, _ := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(2, "t2"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(3, "t3"))
			tasks := mustGetTestDaemonTask(t, s, 3)

			mustDeleteTestDaemonTask(t, s, 0, WithTaskRunnerCond(EQ, "t4"))
			mustDeleteTestDaemonTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))

			mustDeleteTestDaemonTask(t, s, 0, WithTaskIDCond(EQ, tasks[len(tasks)-1].ID+1))
			mustDeleteTestDaemonTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))

			mustGetTestDaemonTask(t, s, 0)
		})
	}
}

func TestQueryDaemonTaskWithConditions(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s, _ := factory(t)
			defer func() {
				require.NoError(t, s.Close())
			}()

			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(1, "t1"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(2, "t2"))
			mustAddTestDaemonTask(t, s, 1, newTestDaemonTask(3, "t3"))
			tasks := mustGetTestDaemonTask(t, s, 3)

			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1))
			mustGetTestDaemonTask(t, s, 1, WithTaskRunnerCond(EQ, "t1"))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(GE, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 3, WithTaskIDCond(LE, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 2, WithTaskIDCond(LT, tasks[2].ID))
			mustGetTestDaemonTask(t, s, 1, WithLimitCond(1), WithTaskIDCond(GT, tasks[0].ID))
			mustGetTestDaemonTask(t, s, 1, WithTaskIDCond(EQ, tasks[0].ID))
		})
	}
}

func mustGetTestAsyncTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []task.AsyncTask {
	tasks, err := s.QueryAsyncTask(context.Background(), conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustAddTestAsyncTask(t *testing.T, s TaskStorage, tasks ...task.AsyncTask) {
	_, err := s.AddAsyncTask(context.Background(), tasks...)
	require.NoError(t, err)
}

func mustUpdateTestAsyncTask(t *testing.T, s TaskStorage, tasks []task.AsyncTask, conds ...Condition) {
	_, err := s.UpdateAsyncTask(context.Background(), tasks, conds...)
	require.NoError(t, err)
}

func mustDeleteTestAsyncTask(t *testing.T, s TaskStorage, conds ...Condition) {
	_, err := s.DeleteAsyncTask(context.Background(), conds...)
	require.NoError(t, err)
}

func mustAddTestCronTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.CronTask) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
	defer cancel()

	n, err := s.AddCronTask(ctx, tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustQueryTestCronTask(t *testing.T, s TaskStorage, expectQueryCount int) []task.CronTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tasks, err := s.QueryCronTask(ctx)
	require.NoError(t, err)
	require.Equal(t, expectQueryCount, len(tasks))
	return tasks
}

func mustAddTestDaemonTask(t *testing.T, s TaskStorage, expectAdded int, tasks ...task.DaemonTask) {
	n, err := s.AddDaemonTask(context.Background(), tasks...)
	require.NoError(t, err)
	require.Equal(t, expectAdded, n)
}

func mustGetTestDaemonTask(t *testing.T, s TaskStorage, expectCount int, conds ...Condition) []task.DaemonTask {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tasks, err := s.QueryDaemonTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectCount, len(tasks))
	return tasks
}

func mustUpdateTestDaemonTask(t *testing.T, s TaskStorage, expectUpdated int, tasks []task.DaemonTask, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.UpdateDaemonTask(ctx, tasks, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func mustDeleteTestDaemonTask(t *testing.T, s TaskStorage, expectUpdated int, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := s.DeleteDaemonTask(ctx, conds...)
	require.NoError(t, err)
	require.Equal(t, expectUpdated, n)
}

func newTestAsyncTask(id string) task.AsyncTask {
	v := task.AsyncTask{}
	v.Metadata.ID = id
	v.TaskRunner = id
	return v
}

func newTestCronTask(id, cron string) task.CronTask {
	v := task.CronTask{}
	v.Metadata.ID = id
	v.CronExpr = cron
	return v
}

func newTestDaemonTask(id uint64, mid string) task.DaemonTask {
	v := task.DaemonTask{Details: new(task.Details)}
	v.ID = id
	v.Metadata.ID = mid
	v.TaskRunner = mid
	return v
}
