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
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
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
			tasks, err := taskService.QueryAsyncTask(context.TODO(),
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
			tasks, err := taskService.QueryAsyncTask(context.TODO(),
				taskservice.WithTaskIDDesc(),
				taskservice.WithTaskStatusCond(task.TaskStatus_Running))
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
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions().
		WithCNServiceNum(1).
		WithCNShardNum(1).
		WithTNServiceNum(1).
		WithTNShardNum(1).
		WithLogServiceNum(3).
		WithLogShardNum(1))
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	t.Log("cluster log svcs length:", len(c.(*testCluster).log.svcs))

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitTNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 1)
	c.WaitLogStoreTaskServiceCreatedIndexed(ctx, 2)
}

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	opt := DefaultOptions()
	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
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
		tasks, err := taskService.QueryAsyncTask(ctx)
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
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	cnSvcNum := 2
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt)
	require.NoError(t, err)

	halt := make(chan bool)
	taskExecutor := func(ctx context.Context, task task.Task) error {
		t.Logf("task %d is running", task.GetID())
		select {
		case <-ctx.Done():
		case <-halt:
		}
		return nil
	}

	defer func(c Cluster, halt chan bool) {
		halt <- true
		require.NoError(t, c.Close())
		close(halt)
	}(c, halt)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 1)
	cn1, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	cn2, err := c.GetCNServiceIndexed(1)
	require.NoError(t, err)
	cn1.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, taskExecutor)
	cn2.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, taskExecutor)

	taskService, ok := cn1.GetTaskService()
	require.True(t, ok)
	err = taskService.CreateAsyncTask(context.TODO(), task.TaskMetadata{ID: "a", Executor: task.TaskCode_TestOnly})
	require.NoError(t, err)

	tasks, err := taskService.QueryAsyncTask(ctx,
		taskservice.WithTaskExecutorCond(taskservice.EQ, task.TaskCode_TestOnly))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	uuid1 := waitTaskScheduled(t, ctx, taskService)

	err = c.CloseCNService(uuid1)
	require.NoError(t, err)

	if uuid1 == cn1.ID() {
		taskService, ok = cn2.GetTaskService()
		require.True(t, ok)
	}
	waitTaskRescheduled(t, ctx, taskService, uuid1)
}

func TestTaskRunner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	ch := make(chan int)
	taskExecutor := func(_ context.Context, task task.Task) error {
		t.Logf("task %d is running", task.GetID())
		ch <- int(task.GetID())
		return nil
	}

	cnSvcNum := 1
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(ctx, t, opt.WithLogLevel(zap.DebugLevel))
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	indexed.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, taskExecutor)

	taskService, ok := indexed.GetTaskService()
	require.True(t, ok)

	err = taskService.CreateAsyncTask(context.TODO(), task.TaskMetadata{ID: "a", Executor: task.TaskCode_TestOnly})
	require.NoError(t, err)

	waitTaskScheduled(t, ctx, taskService)

	select {
	case <-ctx.Done():
		assert.FailNow(t, "task not running")
	case i := <-ch:
		t.Logf("task %d is completed", i)
	}
}

func TestCronTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	opt := DefaultOptions()
	// initialize cluster
	c, err := NewCluster(ctx, t, opt.WithLogLevel(zap.DebugLevel))
	require.NoError(t, err)

	ch := make(chan int)
	taskExecutor := func(ctx context.Context, task task.Task) error {
		t.Logf("task %d is running", task.GetID())
		select {
		case ch <- int(task.GetID()):
		case <-ctx.Done():
			return nil
		}
		return nil
	}

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	indexed.GetTaskRunner().RegisterExecutor(task.TaskCode_TestOnly, taskExecutor)

	taskService, ok := indexed.GetTaskService()
	require.True(t, ok)

	require.NoError(t, taskService.CreateCronTask(context.TODO(),
		task.TaskMetadata{
			ID:       "a",
			Executor: task.TaskCode_TestOnly,
		},
		"*/1 * * * * *", // every 1 second
	))

	waitChannelFull(t, ctx, ch, 3)
}

func waitChannelFull(t *testing.T, ctx context.Context, ch chan int, expected int) {
	i := 0
	received := make([]int, 0, expected)
	for {
		select {
		case <-ctx.Done():
			assert.FailNow(t, "cron task not repeated enough")
		case c := <-ch:
			received = append(received, c)
			if len(received) == expected {
				t.Logf("received %d numbers", expected)
				return
			}
		default:
			t.Logf("iteration: %d", i)
			time.Sleep(time.Second)
			i++
		}
	}
}
