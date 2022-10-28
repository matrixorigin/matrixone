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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))

	v := mustGetTestTask(t, store, 1)[0]
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
	s := NewTaskService(store, nil)
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

	values := mustGetTestTask(t, store, n)
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
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > 0)
	assert.Equal(t, "r1", v.TaskRunner)
	assert.Equal(t, uint32(1), v.Epoch)
}

func TestReAllocate(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > 0)
	assert.Equal(t, "r1", v.TaskRunner)
	assert.Equal(t, uint32(1), v.Epoch)

	last := v.LastHeartbeat
	time.Sleep(time.Millisecond)
	assert.NoError(t, s.Allocate(ctx, v, "r2"))
	v = mustGetTestTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Running, v.Status)
	assert.True(t, v.LastHeartbeat > last)
	assert.Equal(t, "r2", v.TaskRunner)
	assert.Equal(t, uint32(2), v.Epoch)
}

func TestAllocateWithNotExistTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.FailNow(t, "must panic")
	}()
	err := s.Allocate(ctx, task.Task{ID: 1}, "r1")
	require.NoError(t, err)
}

func TestAllocateWithInvalidEpoch(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	store.preUpdate = func() {
		store.Lock()
		defer store.Unlock()

		for k, v := range store.tasks {
			v.Epoch++
			store.tasks[k] = v
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	err := s.Allocate(ctx, v, "r2")
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompleted(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"}))

	v = mustGetTestTask(t, store, 1)[0]
	assert.Equal(t, task.TaskStatus_Completed, v.Status)
	assert.Equal(t, task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"}, *v.ExecuteResult)
}

func TestCompletedWithInvalidStatus(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	v.Status = task.TaskStatus_Created
	mustUpdateTestTask(t, store, 1, []task.Task{v})

	err := s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompletedWithInvalidEpoch(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestTask(t, store, 1, []task.Task{v})

	v.Epoch = 1
	err := s.Complete(ctx, "r1", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestCompletedWithInvalidTaskRunner(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	err := s.Complete(ctx, "r2", v,
		task.ExecuteResult{Code: task.ResultCode_Failed, Error: "error"})
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestHeartbeat(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	lastHeartbeat := v.LastHeartbeat
	time.Sleep(time.Millisecond * 5)
	assert.NoError(t, s.Heartbeat(ctx, v))

	v = mustGetTestTask(t, store, 1)[0]
	assert.True(t, v.LastHeartbeat > lastHeartbeat)
}

func TestHeartbeatWithSmallEpoch(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestTask(t, store, 1, []task.Task{v})

	v.Epoch = 1
	err := s.Heartbeat(ctx, v)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidTask))
}

func TestHeartbeatWithBiggerEpochShouldSuccess(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	assert.NoError(t, s.Create(ctx, newTestTaskMetadata("t1")))
	v := mustGetTestTask(t, store, 1)[0]
	assert.NoError(t, s.Allocate(ctx, v, "r1"))

	v = mustGetTestTask(t, store, 1)[0]
	v.Epoch = 2
	mustUpdateTestTask(t, store, 1, []task.Task{v})

	v.Epoch = 3
	assert.NoError(t, s.Heartbeat(ctx, v))
}

func TestCreateCronTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
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
	s := NewTaskService(store, nil)
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

func TestQueryTask(t *testing.T) {
	store := NewMemTaskStorage()
	s := NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	v1 := newTestTask("t1")
	v2 := newTestTask("t2")
	mustAddTestTask(t, store, 2, v1, v2)

	v, err := s.QueryTask(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(v))
}

func newTestTaskMetadata(id string) task.TaskMetadata {
	return task.TaskMetadata{
		ID: id,
	}
}
