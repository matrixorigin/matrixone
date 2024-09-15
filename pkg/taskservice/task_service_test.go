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
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
)

func TestCreateAsyncTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))

	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.True(t, v.ID > 0)
	assert.True(t, v.CreateAt > 0)
	assert.Equal(t, int64(0), v.CompletedAt)
	assert.Equal(t, task.TaskStatus_Created, v.Status)
	assert.Equal(t, "", v.ParentTaskID)
	assert.Equal(t, "", v.TaskRunner)
	assert.Equal(t, uint32(0), v.Epoch)
	assert.Equal(t, int64(0), v.LastHeartbeat)
	assert.Nil(t, v.ExecuteResult)
	assert.Equal(t, newTestTaskMetadata("t1"), v.Metadata)
}

func TestCreateBatch(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	n := 10
	var tasks []task.TaskMetadata
	for i := 0; i < n; i++ {
		tasks = append(tasks, newTestTaskMetadata(fmt.Sprintf("task-%d", i)))
	}

	assert.NoError(t, s.CreateBatch(ctx, tasks))

	values := mustGetTestAsyncTask(t, store, n)
	for i := 0; i < n; i++ {
		v := values[i]
		assert.True(t, v.ID > 0)
		assert.True(t, v.CreateAt > 0)
		assert.Equal(t, int64(0), v.CompletedAt)
		assert.Equal(t, task.TaskStatus_Created, v.Status)
		assert.Equal(t, "", v.ParentTaskID)
		assert.Equal(t, "", v.TaskRunner)
		assert.Equal(t, uint32(0), v.Epoch)
		assert.Equal(t, int64(0), v.LastHeartbeat)
		assert.Nil(t, v.ExecuteResult)
		assert.Equal(t, newTestTaskMetadata(fmt.Sprintf("task-%d", i)), v.Metadata)
	}
}

func TestAllocate(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > 0)
	assert.Equal(t, "r1", v.TaskRunner)
	assert.Equal(t, uint32(1), v.Epoch)
}

func TestReAllocate(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > 0)
	assert.Equal(t, "r1", v.TaskRunner)
	assert.Equal(t, uint32(1), v.Epoch)

	last := v.LastHeartbeat
	time.Sleep(time.Millisecond)
	assert.NoError(t, s.Allocate(ctx, v, "r2"))
	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > last)
	assert.Equal(t, "r2", v.TaskRunner)
	assert.Equal(t, uint32(2), v.Epoch)
}

func allocateWithNotExistTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	_ = s.Allocate(ctx, task.AsyncTask{ID: 1}, "r1")
}

func TestAllocateWithNotExistTask(t *testing.T) {
	if os.Getenv("RUN_TEST") == "1" {
		allocateWithNotExistTask(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestAllocateWithNotExistTask")
	cmd.Env = append(os.Environ(), "RUN_TEST=1")
	err := cmd.Run()
	// check Fatal is called
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}

func TestAllocateWithInvalidEpoch(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	store.preUpdate = func() {
		store.Lock()
		defer store.Unlock()

		for k, v := range store.asyncTasks {
			v.Epoch++
			store.asyncTasks[k] = v
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	err := s.Allocate(ctx, v, "r2")
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompleted(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"}))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Completed, v.Status)
	assert.Equal(t, task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"}, *v.ExecuteResult)
}

func TestCompletedWithInvalidStatus(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	v.Status = task.TaskStatus_Created
	mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{v})

	err := s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompletedWithInvalidEpoch(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{v})

	v.Epoch = 1
	err := s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompletedWithInvalidTaskRunner(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	err := s.Complete(ctx, "r2", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestHeartbeat(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	lastHeartbeat := v.LastHeartbeat
	time.Sleep(time.Millisecond * 5)
	assert.NoError(t, s.Heartbeat(ctx, v))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	assert.True(t, v.LastHeartbeat > lastHeartbeat)
}

func TestHeartbeatWithSmallEpoch(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{v})

	v.Epoch = 1
	err := s.Heartbeat(ctx, v)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestHeartbeatWithBiggerEpochShouldSuccess(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.CreateAsyncTask(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestAsyncTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestAsyncTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestAsyncTask(t, store, 1, []task.AsyncTask{v})

	v.Epoch = 3
	assert.NoError(t, s.Heartbeat(ctx, v))
}

func TestCreateCronTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "* */5 * * * *"))
	assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "* */5 * * * *"))
}

func TestQueryCronTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestCronTask("t1", "cron1")
	v2 := newTestCronTask("t2", "cron2")
	mustAddTestCronTask(t, store, 2, v1, v2)

	v, err := s.QueryCronTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(v))
}

func TestQueryAsyncTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestAsyncTask("t1")
	v2 := newTestAsyncTask("t2")
	mustAddTestAsyncTask(t, store, 2, v1, v2)

	v, err := s.QueryAsyncTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(v))
}

func newTestTaskMetadata(id string) task.TaskMetadata {
	return task.TaskMetadata{
		ID: id,
	}
}

func TestCreateDaemonTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	assert.NoError(t, s.CreateDaemonTask(ctx, newTestTaskMetadata("t1"),
		&task.Details{
			AccountID: 10,
			Account:   "a1",
			Username:  "u1",
			Details: &task.Details_Connector{
				Connector: &task.ConnectorDetails{
					TableName: "d1.t1",
					Options: map[string]string{
						"k1": "v1",
					},
				},
			},
		}))

	v := mustGetTestDaemonTask(t, store, 1)[0]
	assert.Equal(t, uint64(0), v.ID)
	assert.False(t, v.CreateAt.IsZero())
	assert.Equal(t, task.TaskStatus_Created, v.TaskStatus)
	assert.Equal(t, "", v.TaskRunner)
	assert.True(t, v.LastHeartbeat.IsZero())
	assert.Equal(t, newTestTaskMetadata("t1"), v.Metadata)
	assert.Equal(t, task.TaskType_TypeKafkaSinkConnector, v.TaskType)
	assert.Equal(t, uint32(10), v.Details.AccountID)
	assert.Equal(t, "a1", v.Details.Account)
	assert.Equal(t, "u1", v.Details.Username)
	details, ok := v.Details.Details.(*task.Details_Connector)
	assert.True(t, ok)
	assert.Equal(t, "d1.t1", details.Connector.TableName)
	assert.Equal(t, "v1", details.Connector.Options["k1"])
}

func TestQueryDaemonTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestDaemonTask(1, "t1")
	v2 := newTestDaemonTask(2, "t2")
	mustAddTestDaemonTask(t, store, 2, v1, v2)

	v, err := s.QueryDaemonTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(v))
}

func TestUpdateDaemonTaskTS(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestDaemonTask(1, "t1")
	mustAddTestDaemonTask(t, store, 1, v1)

	v1.TaskStatus = task.TaskStatus_Running

	c, err := s.UpdateDaemonTask(ctx, []task.DaemonTask{v1})
	assert.NoError(t, err)
	assert.Equal(t, 1, c)

	ts, err := s.QueryDaemonTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ts))
	assert.Equal(t, task.TaskStatus_Running, ts[0].TaskStatus)
}

func TestHeartbeatDaemonTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestDaemonTask(1, "t1")
	v1.TaskStatus = task.TaskStatus_Running
	mustAddTestDaemonTask(t, store, 1, v1)

	v1.LastHeartbeat = time.Now()

	err := s.HeartbeatDaemonTask(ctx, v1)
	assert.NoError(t, err)

	ts, err := s.QueryDaemonTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ts))
	assert.False(t, ts[0].LastHeartbeat.IsZero())
}

func TestAddCdcTask1(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

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

	cnt, err := s.AddCdcTask(
		ctx,
		newTestTaskMetadata("t1"),
		details,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, cnt, 0)

	cnt, err = s.UpdateCdcTask(context.Background(), task.TaskStatus_Canceled, nil)
	assert.NoError(t, err)
	assert.Equal(t, cnt, 0)

	_ = s.Close()
	_ = store.Close()
}

func Test_conditions(t *testing.T) {

	conds := []Condition{
		WithTaskExecutorCond(EQ, task.TaskCode_InitCdc),
		WithTaskType(EQ, task.TaskType_CreateCdc.String()),
		WithAccountID(EQ, catalog.System_Account),
		WithAccount(EQ, "sys"),
		WithLastHeartbeat(EQ, 10),
		WithCronTaskId(EQ, 10),
		WithTaskMetadataId(EQ, "taskID-1"),
		WithTaskName(EQ, "task1"),
		WithLabels(EQ, nil),
		WithTaskIDDesc(),
	}

	condObj := newConditions(conds...)

	for _, cond := range *condObj {
		cond.sql()
		cond.eval(nil)
	}
}
