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
		tasks     []task.AsyncTask
		workingCN pb.CNState

		expected map[uint64]struct{}
	}{
		{
			tasks:     nil,
			workingCN: pb.CNState{},

			expected: nil,
		},
		{
			// CN running task 1 is expired.
			tasks: []task.AsyncTask{
				{ID: 1, TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()},
				{ID: 2, TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
			},
			workingCN: pb.CNState{Stores: map[string]pb.CNStoreInfo{"b": {}}},

			expected: map[uint64]struct{}{1: {}},
		},
		{
			// Heartbeat of task 1 is expired.
			tasks: []task.AsyncTask{
				{ID: 1, TaskRunner: "a", LastHeartbeat: time.Now().Add(-taskSchedulerDefaultTimeout - 1).UnixMilli()},
				{ID: 2, TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
			},
			workingCN: pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {}}},

			expected: map[uint64]struct{}{1: {}},
		},
	}

	for _, c := range cases {
		results := getExpiredTasks(c.tasks, newCNPoolWithCNState(c.workingCN))
		for _, asyncTask := range results {
			_, ok := c.expected[asyncTask.ID]
			assert.True(t, ok)
		}
	}
}

func TestGetCNOrderedMap(t *testing.T) {
	cases := []struct {
		tasks     []task.AsyncTask
		workingCN pb.CNState

		expected *cnPool
	}{
		{
			tasks:     nil,
			workingCN: pb.CNState{},

			expected: newCNPool(),
		},
		{
			tasks: []task.AsyncTask{
				{TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()},
				{TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
				{TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()}},
			workingCN: pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {}}},

			expected: &cnPool{
				freq:     map[string]uint32{"a": 1, "b": 2},
				sortedCN: []cnStore{{uuid: "a", info: pb.CNStoreInfo{}}, {uuid: "b", info: pb.CNStoreInfo{}}},
			},
		},
		{
			tasks: []task.AsyncTask{
				{TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()},
				{TaskRunner: "b", LastHeartbeat: time.Now().UnixMilli()},
				{TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()},
				{TaskRunner: "a", LastHeartbeat: time.Now().UnixMilli()}},
			workingCN: pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {}}},

			expected: &cnPool{
				freq:     map[string]uint32{"a": 3, "b": 1},
				sortedCN: []cnStore{{uuid: "b", info: pb.CNStoreInfo{}}, {uuid: "a", info: pb.CNStoreInfo{}}},
			},
		},
	}

	for _, c := range cases {
		pool := newCNPoolWithCNState(c.workingCN)
		getExpiredTasks(c.tasks, pool)
		assert.Equal(t, c.expected, pool)
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
	assert.NoError(t, service.CreateAsyncTask(context.Background(), task.TaskMetadata{ID: "1"}))
	query, err := service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Create Task 2
	assert.NoError(t, service.CreateAsyncTask(context.Background(), task.TaskMetadata{ID: "2"}))
	query, err = service.QueryAsyncTask(context.Background(),
		taskservice.WithTaskStatusCond(task.TaskStatus_Created))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.NotNil(t, query[0].Status)

	// Add CNStore "b"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {}}}

	// Schedule Task 2
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background(), taskservice.WithTaskRunnerCond(taskservice.EQ, "b"))
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
	assert.NoError(t, service.CreateAsyncTask(context.Background(), task.TaskMetadata{ID: "1"}))
	query, err := service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1 on "a"
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
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

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Add CNStore "b"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{"a": {}, "b": {Tick: expiredTick}}}

	// Re-schedule Task 1
	// "b" available
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "b", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)
}

func TestAllocTasksWithLabels(t *testing.T) {
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	scheduler := NewScheduler(func() taskservice.TaskService { return service }, hakeeper.Config{})
	cnState := pb.CNState{Stores: map[string]pb.CNStoreInfo{
		"a": {Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1"}}}},
		"b": {Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v2"}}}},
	}}
	currentTick := expiredTick - 1

	// Create Task 1
	assert.NoError(t, service.CreateAsyncTask(context.Background(), task.TaskMetadata{ID: "1", Options: task.TaskOptions{Labels: map[string]string{"k1": "v1"}}}))
	query, err := service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1 on "a"
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
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

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Add CNStore "c"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{
		"a": {},
		"b": {Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v2"}}}},
		"c": {
			Tick:   expiredTick,
			Labels: map[string]metadata.LabelList{"k1": {Labels: []string{"v1"}}},
		},
	}}

	// Re-schedule Task 1
	// "c" available
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "c", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)
}

func TestAllocTasksWithMemoryOrCPU(t *testing.T) {
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	scheduler := NewScheduler(func() taskservice.TaskService { return service }, hakeeper.Config{})
	cnState := pb.CNState{Stores: map[string]pb.CNStoreInfo{
		"a": {Resource: pb.Resource{CPUTotal: 1, MemTotal: 200}},
		"b": {Resource: pb.Resource{CPUTotal: 1, MemTotal: 100}},
	}}
	currentTick := expiredTick - 1

	// Create Task 1
	assert.NoError(t, service.CreateAsyncTask(context.Background(), task.TaskMetadata{ID: "1",
		Options: task.TaskOptions{Resource: &task.Resource{CPU: 1, Memory: 150}}}))
	query, err := service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, task.TaskStatus_Created, query[0].Status)

	// Schedule Task 1 on "a"
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
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

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "a", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)

	// Add CNStore "c"
	cnState = pb.CNState{Stores: map[string]pb.CNStoreInfo{
		"a": {},
		"b": {},
		"c": {Tick: expiredTick, Resource: pb.Resource{CPUTotal: 2, MemTotal: 200}},
	}}

	// Re-schedule Task 1
	// "c" available
	scheduler.Schedule(cnState, currentTick)

	query, err = service.QueryAsyncTask(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(query))
	assert.Equal(t, "c", query[0].TaskRunner)
	assert.Equal(t, task.TaskStatus_Running, query[0].Status)
}
