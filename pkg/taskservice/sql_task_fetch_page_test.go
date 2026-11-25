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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskScanPageHandlesUnknownHeaders(t *testing.T) {
	store, mock := newMockStorage(t)
	query := "select task_id, task_status, task_runner, task_metadata_executor, task_parent_id, sql_task_run_id from sys_async_task where task_id>? and task_id<=? order by task_id asc limit 32"
	rows := sqlmock.NewRows([]string{"task_id", "task_status", "task_runner", "task_metadata_executor", "task_parent_id", "sql_task_run_id"}).
		AddRow(1, 1, "cn1", 12, "sql-task:9", 0).
		AddRow(2, nil, "cn1", 12, "sql-task:9", 0).
		AddRow(3, "bad", "cn1", 12, "sql-task:9", 0).
		AddRow(4, 1, "cn1", "4294967296", "sql-task:9", 0).
		AddRow(5, 1, nil, 12, "sql-task:9", 0).
		AddRow(6, 1, "cn1", 12, "sql-task:9", -1).
		AddRow(7, 99, "cn1", 12, "sql-task:9", 0)
	mock.ExpectQuery(query).WithArgs(uint64(0), uint64(7)).WillReturnRows(rows)
	page, err := store.ScanAsyncTaskPage(context.Background(), 0, 7, false, 100)
	require.NoError(t, err)
	require.Len(t, page, 7)
	require.Equal(t, AsyncTaskScanRow{ID: 1, Status: task.TaskStatus_Running, Runner: "cn1", Executor: task.TaskCode_SQLTask, Parent: "sql-task:9", Known: true}, page[0])
	for _, row := range page[1:] {
		require.False(t, row.Known, "row %d cannot safely skip the decorated lookup", row.ID)
	}
	mock.ExpectClose()
	require.NoError(t, store.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

type unknownSQLTaskScanStore struct {
	*memTaskStorage
	queries int
}

func (s *unknownSQLTaskScanStore) ScanAsyncTaskPage(ctx context.Context, after, high uint64, desc bool, limit int) ([]AsyncTaskScanRow, error) {
	rows, err := s.memTaskStorage.ScanAsyncTaskPage(ctx, after, high, desc, limit)
	for i := range rows {
		rows[i].Known = false
	}
	return rows, err
}

func (s *unknownSQLTaskScanStore) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	if _, ok := (*newConditions(conds...))[CondTaskID].(*taskIDCond); ok {
		s.queries++
	}
	return s.memTaskStorage.QueryAsyncTask(ctx, conds...)
}

func TestSQLTaskRawPageChecksUnknownHeaders(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &unknownSQLTaskScanStore{memTaskStorage: base}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "eligible", Executor: task.TaskCode_TestOnly}})
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1)).(*taskRunner)
	cursor := sqlTaskFetchCursor{high: 1, ready: true}
	values, err := runner.fetchSQLTaskRawPage(context.Background(), service.(*taskService), &cursor, false, 1, nil)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "eligible", values[0].Metadata.ID)
	require.Equal(t, 1, store.queries)
}

type blockedNormalQueryStore struct {
	TaskStorage
	entered chan struct{}
	release chan struct{}
}

func (s *blockedNormalQueryStore) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	close(s.entered)
	select {
	case <-s.release:
		return s.TaskStorage.QueryAsyncTask(ctx, conds...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSQLTaskNormalPageTrustTracksQueriedHolderStore(t *testing.T) {
	base := NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	blocked := &blockedNormalQueryStore{TaskStorage: base, entered: make(chan struct{}), release: make(chan struct{})}
	holder := &refreshableTaskStorage{refreshC: make(chan string, 1)}
	holder.mu.store = blocked
	result := make(chan bool, 1)
	go func() {
		_, err, trusted := holder.queryAsyncTaskPage(context.Background())
		result <- err == nil && trusted
	}()
	<-blocked.entered
	swapped := make(chan struct{})
	go func() {
		holder.mu.Lock()
		holder.mu.store = base
		holder.mu.Unlock()
		close(swapped)
	}()
	close(blocked.release)
	require.False(t, <-result, "the query used the decorator even if the holder is refreshed")
	<-swapped
	_, err, trusted := holder.queryAsyncTaskPage(context.Background())
	require.NoError(t, err)
	require.True(t, trusted, "the next query used the built-in store")
}
