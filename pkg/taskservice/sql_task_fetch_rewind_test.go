// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskRejectedNewLaneSurvivesWaveRollover(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage()
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(1)).(*taskRunner)
	_, err := store.AddAsyncTask(ctx,
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "running-prefix", Executor: task.TaskCode_TestOnly}},
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:9", Metadata: task.TaskMetadata{ID: "new-candidate", Executor: task.TaskCode_SQLTask}},
	)
	require.NoError(t, err)
	all, err := store.QueryAsyncTask(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	runner.runningTasks.m[all[0].ID] = runningTask{task: all[0], ctx: ctx}
	runner.sqlFetch = sqlTaskFetchState{active: true, waveReady: true, waveHigh: all[0].ID,
		other: sqlTaskFetchCursor{initialDone: true}, old: sqlTaskFetchCursor{initialDone: true},
		new: sqlTaskFetchCursor{after: all[1].ID, high: all[1].ID, ready: true}}
	runner.sqlFetch.last = &runner.sqlFetch.new
	runner.sqlFetch.rewind(all[1].ID) // the selected new-lane task did not fit in the wait queue
	require.True(t, runner.useSQLTaskFairFetch(service.(*taskService)), "a rejected new-lane candidate must keep fair recovery active across rollover")
	for i := 0; i < 12; i++ {
		values, err := runner.doFetch(ctx, ctx)
		require.NoError(t, err)
		for _, value := range values {
			if value.Metadata.ID == "new-candidate" {
				return
			}
		}
	}
	t.Fatal("the rejected new-lane task was lost at wave rollover behind a running LIMIT prefix")
}
