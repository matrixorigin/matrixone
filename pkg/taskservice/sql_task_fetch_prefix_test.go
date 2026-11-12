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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskNormalFetchReentersForRunningPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := NewMemTaskStorage()
		service := NewTaskService(runtime.DefaultRuntime(), store)
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(1), WithRunnerFetchInterval(200*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, service.Close())
		})
		started, target := make(chan struct{}), make(chan struct{}, 1)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { close(started); <-release; return nil })
		runner.RegisterExecutor(task.TaskCode_SQLTask, func(context.Context, task.Task) error { target <- struct{}{}; return nil })
		_, err := store.AddAsyncTask(ctx, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "first", Executor: task.TaskCode_TestOnly}})
		require.NoError(t, err)
		require.NoError(t, runner.Start())
		<-started
		spec := &task.SQLTaskContext{TaskId: 9, TriggerType: SQLTaskTriggerScheduled, TriggerCount: 1}
		_, err = store.AddAsyncTask(ctx, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1,
			ParentTaskID: "sql-task:9", Metadata: BuildSQLTaskMetadata(spec)})
		require.NoError(t, err)
		for i := 0; i < 8; i++ {
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()
			select {
			case <-target:
				return
			default:
			}
		}
		t.Fatal("an already-running LIMIT prefix hid an eligible task while capacity remained")
	})
}
