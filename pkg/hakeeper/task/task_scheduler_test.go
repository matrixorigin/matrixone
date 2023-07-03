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

package task

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	runtime.SetupProcessLevelRuntime(runtime.NewRuntime(metadata.ServiceType_LOG, "test", logutil.GetGlobalLogger()))
	m.Run()
}

func TestGetExpiredTasks(t *testing.T) {
	cases := []struct {
		tasks     []task.Task
		workingCN []string

		expected map[uint64]struct{}
	}{
		{
			tasks:     nil,
			workingCN: nil,

			expected: nil,
		},
		{
			// CN running task 1 is expired.
			tasks: []task.Task{
				{ID: 1, TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()},
				{ID: 2, TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
			},
			workingCN: []string{"b"},

			expected: map[uint64]struct{}{1: {}},
		},
		{
			// Heartbeat of task 1 is expired.
			tasks: []task.Task{
				{ID: 1, TaskRunner: "a", LastHeartbeat: time.Now().Add(-taskSchedulerDefaultTimeout - 1).UnixMilli()},
				{ID: 2, TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
			},
			workingCN: []string{"a", "b"},

			expected: map[uint64]struct{}{1: {}},
		},
	}

	for _, c := range cases {
		_, results := getCNOrderedAndExpiredTasks(c.tasks, c.workingCN)
		for _, task := range results {
			_, ok := c.expected[task.ID]
			assert.True(t, ok)
		}
	}
}

func TestGetCNOrderedMap(t *testing.T) {
	cases := []struct {
		tasks     []task.Task
		workingCN []string

		expected *cnMap
	}{
		{
			tasks:     nil,
			workingCN: nil,

			expected: newOrderedMap(nil),
		},
		{
			tasks:     []task.Task{{TaskRunner: "a"}, {TaskRunner: "b"}, {TaskRunner: "b"}},
			workingCN: []string{"a", "b"},

			expected: &cnMap{
				m:           map[string]uint32{"a": 1, "b": 2},
				orderedKeys: []string{"a", "b"},
			},
		},
		{
			tasks:     []task.Task{{TaskRunner: "a"}, {TaskRunner: "b"}, {TaskRunner: "a"}, {TaskRunner: "a"}},
			workingCN: []string{"a", "b"},

			expected: &cnMap{
				m:           map[string]uint32{"a": 3, "b": 1},
				orderedKeys: []string{"b", "a"},
			},
		},
	}

	for _, c := range cases {
		results, _ := getCNOrderedAndExpiredTasks(c.tasks, c.workingCN)
		assert.Equal(t, c.expected, results)
	}
}

func TestScheduleCreatedTasks(t *testing.T) {
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	scheduler := NewScheduler(func() taskservice.TaskService { return service }, hakeeper.Config{})
	cnState := pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}}
	currentTick := uint64(0)

	// Schedule empty task
	scheduler.Schedule(cnState, currentTick)

	// Create Task 1
	assert.NoError(t, service.Create(context.Background(), task.TaskMetadata{ID: "1"}))
	query, err := service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Create Task 2
	assert.NoError(t, service.Create(context.Background(), task.TaskMetadata{ID: "2"}))
	query, err = service.QueryTask(context.Background(),
		taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Created))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.NotNil(t, query[0].Status)

	// Add CNStore "b"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {}}}

	// Schedule Task 2
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryTask(context.Background(), taskservice.WithTaskRunnerCond(taskservice.EQ, "b"))
	assert.NoError(t, err)
	assert.NotNil(t, query)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)
}

func TestReallocateExpiredTasks(t *testing.T) {
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	scheduler := NewScheduler(func() taskservice.TaskService { return service }, hakeeper.Config{})
	cnState := pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}}
	currentTick := expiredTick - 1

	// Create Task 1
	assert.NoError(t, service.Create(context.Background(), task.TaskMetadata{ID: "1"}))
	query, err := service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1 on "a"
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Make CNStore "a" expired
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}}
	currentTick = expiredTick + 1

	// Re-schedule Task 1
	// Since no other CN available, task 1 remains on CN "a"
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Add CNStore "b"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {Tick: expiredTick}}}

	// Re-schedule Task 1
	// "b" available
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "b", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)
}

func TestSchedulerCreateTasks(t *testing.T) {
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	scheduler := NewScheduler(func() taskservice.TaskService { return service }, hakeeper.Config{})
	cnState := pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}}}
	currentTick := uint64(0)

	assert.NoError(t, scheduler.Create(context.Background(),
		[]task.TaskMetadata{{ID: "1"}}))

	// Schedule empty task
	scheduler.Schedule(cnState, currentTick)
}
