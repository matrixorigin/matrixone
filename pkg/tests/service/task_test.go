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

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
			require.NoError(t, err)

			if len(tasks) == 1 {
				t.Logf("task %d allocated on %s", tasks[0].ID, tasks[0].TaskRunner)
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
				taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
			require.NoError(t, err)
			require.Equal(t, 1, len(tasks))
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

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	cnSvcNum := 1
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum).
		WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	indexed.GetTaskRunner().RegisterExecutor(0,
		func(ctx context.Context, task task.Task) error {
			return nil
		},
	)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "a", Executor: 0})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitTaskScheduled(t, ctx, taskService)
	err = c.Close()
	require.NoError(t, err)
}

func TestTaskSchedulerCanReallocateTask(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	cnSvcNum := 2
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum).
		WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	cn1, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	cn1.GetTaskRunner().RegisterExecutor(0,
		func(ctx context.Context, task task.Task) error {
			return nil
		},
	)

	cn2, err := c.GetCNServiceIndexed(1)
	require.NoError(t, err)
	cn2.GetTaskRunner().RegisterExecutor(0,
		func(ctx context.Context, task task.Task) error {
			return nil
		},
	)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "a", Executor: 0})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	uuid1 := waitTaskScheduled(t, ctx, taskService)

	err = c.CloseCNService(uuid1)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()
	waitTaskRescheduled(t, ctx1, taskService, uuid1)

	err = c.Close()
	require.NoError(t, err)
}

func TestTaskRunner(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	ch := make(chan int)
	taskExecutor := func(ctx context.Context, task task.Task) error {
		t.Logf("task %d is running", task.ID)
		ch <- int(task.ID)
		return nil
	}

	cnSvcNum := 1
	opt := DefaultOptions().
		WithCNServiceNum(cnSvcNum).
		WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	indexed.GetTaskRunner().RegisterExecutor(1, taskExecutor)
	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "a", Executor: 1})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitTaskScheduled(t, ctx, taskService)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel1()
	select {
	case <-ctx1.Done():
		assert.FailNow(t, "task not running")
	case i := <-ch:
		t.Logf("task %d is completed", i)
	}
	err = c.Close()
	require.NoError(t, err)
}
