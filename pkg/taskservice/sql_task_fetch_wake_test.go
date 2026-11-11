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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

type countedWakeStore struct {
	TaskStorage
	calls atomic.Int32
}

func (s *countedWakeStore) ScanAsyncTaskPage(ctx context.Context, after, high uint64, desc bool, limit int) ([]AsyncTaskScanRow, error) {
	s.calls.Add(1)
	return s.TaskStorage.ScanAsyncTaskPage(ctx, after, high, desc, limit)
}

type emptyWakeService struct {
	TaskService
	store *countedWakeStore
}

func (s *emptyWakeService) QueryAsyncTask(context.Context, ...Condition) ([]task.AsyncTask, error) {
	s.store.calls.Add(1)
	return nil, nil
}

func TestSQLTaskFetchWakeBudgetAndTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &countedWakeStore{TaskStorage: NewMemTaskStorage()}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &emptyWakeService{TaskService: owner, store: store}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchInterval(200*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		require.NoError(t, runner.Start())
		for range 12 {
			runner.sqlFetchWake <- struct{}{}
			synctest.Wait()
		}
		before := store.calls.Load()
		require.Positive(t, before)
		require.LessOrEqual(t, before, int32(32), "hints must not create unbounded fetch work")
		// Empty fair lanes need not query on every poll. Allow one rotation.
		for range 4 {
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()
		}
		require.Greater(t, store.calls.Load(), before, "the periodic poll must remain active after the hint budget is exhausted")
	})
}
