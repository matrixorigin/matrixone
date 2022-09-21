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
	"github.com/stretchr/testify/require"
)

func TestTaskSchedulerCanAllocateTask(t *testing.T) {
	taskService := taskservice.NewTaskService(taskservice.NewMemTaskStorage(), nil)

	dnSvcNum := 1
	cnSvcNum := 1
	opt := DefaultOptions().
		WithDNServiceNum(dnSvcNum).
		WithCNServiceNum(cnSvcNum)

	// initialize cluster
	c, err := NewCluster(t, opt, taskService)
	require.NoError(t, err)

	// start the cluster
	err = c.Start()
	require.NoError(t, err)

	err = taskService.Create(context.TODO(), task.TaskMetadata{ID: "a"})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		t.Logf("iteration: %d", i)
		tasks, err := taskService.QueryTask(context.TODO(),
			taskservice.WithTaskStatusCond(taskservice.EQ, task.TaskStatus_Running))
		require.NoError(t, err)

		if len(tasks) == 1 {
			t.Logf("task %d allocated on %s", tasks[0].ID, tasks[0].TaskRunner)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("task not allocated")
}
