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

package service

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func waitTaskScheduled(t *testing.T, ctx context.Context, taskService taskservice.TaskService) string {
	i := 0
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(t, "task not allocated")
		default:
			t.Logf("iteration: %d", i)
			tasks, err := taskService.QueryTask(context.TODO(),
				taskservice.WithTaskIDDesc())
			require.NoError(t, err)

			if len(tasks) != 0 && tasks[0].TaskRunner != "" {
				t.Logf("task %d allocated on %s", tasks[0].ID, tasks[0].TaskRunner)
				t.Logf("num task: %d", len(tasks))
				return tasks[0].TaskRunner
			}
			time.Sleep(300 * time.Millisecond)
			i++
		}
	}
}

func waitTaskRescheduled(t *testing.T, ctx context.Context, taskService taskservice.TaskService, uuid string) {
	i := 0
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(t, "task not reallocated")
		default:
			t.Logf("iteration: %d", i)
			tasks, err := taskService.QueryTask(context.TODO(),
				taskservice.WithTaskIDDesc(),
				taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
			require.NoError(t, err)
			if tasks[0].TaskRunner == uuid {
				t.Logf("task %d is still on %s", tasks[0].ID, tasks[0].TaskRunner)
				time.Sleep(1 * time.Second)
				i++
				continue
			} else {
				t.Logf("task %d reallocated on %s", tasks[0].ID, tasks[0].TaskRunner)
				return
			}
		}
	}
}

func TestTaskServiceCanCreate(t *testing.T) {
	// initialize cluster
	c, err := NewCluster(t, DefaultOptions().
		WithCNServiceNum(1).
		WithCNShardNum(1).
		WithDNServiceNum(1).
		WithDNShardNum(1).
		WithLogServiceNum(3).
		WithLogShardNum(1))
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)
	defer func(c Cluster) {
		_ = c.Close()
	}(c)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitDNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 0)
}

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	cnSvcNum := 1
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)
	defer func(c Cluster) {
		_ = c.Close()
	}(c)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	taskService, ok := indexed.GetTaskService()
	require.True(t, ok)

	i := 0
	for {
		select {
		case <-ctx.Done():
			require.FailNow(t, "failed to query tasks")
		default:
		}
		t.Logf("iter %d", i)
		tasks, err := taskService.QueryTask(ctx)
		require.NoError(t, err)
		if len(tasks) == 0 {
			time.Sleep(time.Second)
			i++
			continue
		}
		require.Equal(t, 1, len(tasks))
		t.Logf("task status: %s", tasks[0].Status)
		break
	}

	waitTaskScheduled(t, ctx, taskService)
}

func TestTaskSchedulerCanReallocateTask(t *testing.T) {
	cnSvcNum := 2
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 1)
	cn1, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	cn2, err := c.GetCNServiceIndexed(1)
	require.NoError(t, err)

	taskService, ok := cn1.GetTaskService()
	require.True(t, ok)
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(ctx,
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	uuid1 := waitTaskScheduled(t, ctx, taskService)

	err = c.CloseCNService(uuid1)
	require.NoError(t, err)

	if uuid1 == cn1.ID() {
		taskService, ok = cn2.GetTaskService()
		require.True(t, ok)
		require.NoError(t, err)
	}
	waitTaskRescheduled(t, ctx, taskService, uuid1)

	err = c.Close()
	require.NoError(t, err)
}

func TestTaskRunner(t *testing.T) {
	ch := make(chan int)
	taskExecutor := func(ctx context.Context, task task.Task) error {
		t.Logf("task %d is running", task.ID)
		ch <- int(task.ID)
		return nil
	}

	cnSvcNum := 1
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt.WithLogLevel(zap.DebugLevel))
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	id := uint32(math.MaxInt32)
	indexed.GetTaskRunner().RegisterExecutor(id, taskExecutor)

	taskService, ok := indexed.GetTaskService()
	require.True(t, ok)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "a", Executor: id})
	require.NoError(t, err)

	waitTaskScheduled(t, ctx, taskService)

	select {
	case <-ctx.Done():
		assert.FailNow(t, "task not running")
	case i := <-ch:
		t.Logf("task %d is completed", i)
	}
	err = c.Close()
	require.NoError(t, err)
}
