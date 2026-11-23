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
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

type allocateAfterRawScanStore struct {
	*memTaskStorage
	id        uint64
	allocated bool
}

func (s *allocateAfterRawScanStore) ScanAsyncTaskPage(ctx context.Context, after, high uint64, desc bool, limit int) ([]AsyncTaskScanRow, error) {
	ids, err := s.memTaskStorage.ScanAsyncTaskPage(ctx, after, high, desc, limit)
	if err != nil || desc || s.allocated {
		return ids, err
	}
	s.allocated = true
	values, err := s.memTaskStorage.QueryAsyncTask(ctx, WithTaskIDCond(EQ, s.id))
	if err != nil || len(values) != 1 {
		return nil, err
	}
	values[0].Status, values[0].TaskRunner = task.TaskStatus_Running, "cn1"
	_, err = s.memTaskStorage.UpdateAsyncTask(ctx, values, WithTaskIDCond(EQ, s.id))
	return ids, err
}

type postLimitTaskService struct{ TaskService }

func (s *postLimitTaskService) QueryAsyncTask(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error) {
	values, err := s.TaskService.QueryAsyncTask(ctx, conditions...)
	if err != nil {
		return nil, err
	}
	filtered := values[:0]
	for _, value := range values {
		if !strings.HasPrefix(value.Metadata.ID, "hidden-") {
			filtered = append(filtered, value)
		}
	}
	return filtered, nil
}

func TestSQLTaskPostFilterAllocationAfterRawSnapshot(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage().(*memTaskStorage)
	mustAddTestAsyncTask(t, base, 2,
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "hidden-1", Executor: task.TaskCode_TestOnly}},
		task.AsyncTask{Status: task.TaskStatus_Created, Metadata: task.TaskMetadata{ID: "becomes-eligible", Executor: task.TaskCode_TestOnly}},
	)
	all := mustGetTestAsyncTask(t, base, 2)
	store := &allocateAfterRawScanStore{memTaskStorage: base, id: all[1].ID}
	owner := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	wrapped := &postLimitTaskService{TaskService: owner}
	runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerFetchLimit(1)).(*taskRunner)
	// A normal LIMIT-1 query is entirely hidden by the decorator.
	normal, err := wrapped.QueryAsyncTask(ctx, WithTaskStatusCond(task.TaskStatus_Running), WithTaskRunnerCond(EQ, "cn1"), WithLimitCond(1))
	require.NoError(t, err)
	require.Empty(t, normal)
	// The snapshot still says Created when the row becomes eligible. A later
	// pass must observe the new state through the decorated query.
	cursor := sqlTaskFetchCursor{high: all[1].ID, ready: true}
	values, err := runner.fetchSQLTaskRawPage(ctx, owner.(*taskService), &cursor, false, 1, nil)
	require.NoError(t, err)
	require.Empty(t, values)
	cursor = sqlTaskFetchCursor{high: all[1].ID, ready: true}
	values, err = runner.fetchSQLTaskRawPage(ctx, owner.(*taskService), &cursor, false, 1, nil)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "becomes-eligible", values[0].Metadata.ID)
}

func TestSQLTaskPostFilterRevisitsLateAllocation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := NewMemTaskStorage().(*memTaskStorage)
		mustAddTestAsyncTask(t, base, 2,
			task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "hidden-1", Executor: task.TaskCode_TestOnly}},
			task.AsyncTask{Status: task.TaskStatus_Created, Metadata: task.TaskMetadata{ID: "becomes-eligible", Executor: task.TaskCode_TestOnly}},
		)
		all := mustGetTestAsyncTask(t, base, 2)
		store := &allocateAfterRawScanStore{memTaskStorage: base, id: all[1].ID}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		wrapped := &postLimitTaskService{TaskService: owner}
		runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(1), WithRunnerFetchInterval(200*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		found := make(chan string, 1)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error { found <- value.GetMetadata().ID; return nil })
		require.NoError(t, runner.Start())
		for i := 0; i < 20; i++ {
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()
			select {
			case id := <-found:
				require.Equal(t, "becomes-eligible", id)
				return
			default:
			}
		}
		t.Fatal("post-LIMIT filtering prevented rediscovery of the later allocation")
	})
}
