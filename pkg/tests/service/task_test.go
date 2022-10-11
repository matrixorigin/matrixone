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
			tasks, err := taskService.QueryTask(context.TODO())
			require.NoError(t, err)
			require.Equal(t, 2, len(tasks))

			for _, ts := range tasks {
				if ts.Metadata.ID == "test_only" && ts.TaskRunner != "" {
					t.Logf("task %d allocated on %s", ts.ID, ts.TaskRunner)
					return ts.TaskRunner
				}
			}

			time.Sleep(500 * time.Millisecond)
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
			tasks, err := taskService.QueryTask(context.TODO())
			require.NoError(t, err)
			require.Equal(t, 2, len(tasks))
			for _, ts := range tasks {
				if ts.Metadata.ID == "test_only" {
					if ts.TaskRunner == uuid {
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
	}
}

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	opt := DefaultOptions().WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	defer func(c Cluster) {
		_ = c.Close()
	}(c)
	require.NoError(t, err)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "test_only", Executor: taskservice.TestOnly})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	waitTaskScheduled(t, ctx, taskService)
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
	defer func(c Cluster) {
		_ = c.Close()
	}(c)
	require.NoError(t, err)

	cn1, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	cn1.GetTaskRunner().RegisterExecutor(taskservice.TestOnly,
		func(ctx context.Context, task task.Task) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
					time.Sleep(1 * time.Second)
				}
			}
		},
	)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "test_only", Executor: taskservice.TestOnly})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	uuid1 := waitTaskScheduled(t, ctx, taskService)

	err = c.CloseCNService(uuid1)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()
	waitTaskRescheduled(t, ctx1, taskService, uuid1)
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

	opt := DefaultOptions().WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	defer func(c Cluster) {
		_ = c.Close()
	}(c)
	require.NoError(t, err)

	indexed, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)

	indexed.GetTaskRunner().RegisterExecutor(taskservice.TestOnly, taskExecutor)
	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "test_only", Executor: taskservice.TestOnly})
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
}

func TestNoErrorOccursWhenTaskRunnerWithNoExecutor(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	opt := DefaultOptions().WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	defer func(c Cluster) {
		_ = c.Close()
	}(c)
	require.NoError(t, err)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "test_only", Executor: taskservice.TestOnly})
	require.NoError(t, err)
	tasks, err := taskService.QueryTask(context.TODO(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	require.NoError(t, err)
	require.Equal(t, 1, len(tasks))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	waitTaskScheduled(t, ctx, taskService)
	// make sure task is not running.
	time.Sleep(3 * time.Second)
}

func TestSysTablesCanInit(t *testing.T) {
	taskStorage := taskservice.NewMemTaskStorage()
	taskService := taskservice.NewTaskService(taskStorage, nil)

	opt := DefaultOptions().WithTaskStorage(taskStorage)

	// initialize cluster
	c, err := NewCluster(t, opt)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	defer func(c Cluster) {
		_ = c.Close()
	}(c)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	waitInitTasksCompleted(t, ctx, taskService)
}

func waitInitTasksCompleted(t *testing.T, ctx context.Context, taskService taskservice.TaskService) {
	i := 0
	fn := func(t *testing.T, status task.TaskStatus) int {
		tasks, err := taskService.QueryTask(context.TODO(),
			taskservice.WithTaskStatusCond(taskservice.EQ, status))
		require.NoError(t, err)
		require.True(t, len(tasks) <= 1)
		taskNames := make([]string, 0, len(tasks))
		for _, ts := range tasks {
			taskNames = append(taskNames, ts.Metadata.ID)
		}

		t.Logf("%s tasks: %v(%d in total)", status.String(), taskNames, len(taskNames))
		return len(taskNames)
	}

	for {
		select {
		case <-ctx.Done():
			assert.FailNow(t, "tasks not completed")
		default:
			t.Logf("iteration: %d", i)
			fn(t, task.TaskStatus_Created)
			fn(t, task.TaskStatus_Running)
			if completed := fn(t, task.TaskStatus_Completed); completed != 1 {
				time.Sleep(1 * time.Second)
				i++
				continue
			}
			t.Logf("all init tasks are completed")
			return
		}
	}
}
