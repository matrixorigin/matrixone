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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

type failingSQLTaskFetchStore struct {
	*memTaskStorage
	poison     uint64
	failNormal bool
}

type transparentSQLFetchService struct{ TaskService }
type transparentSQLFetchStorage struct{ TaskStorage }

type switchableSQLTaskOwner struct {
	TaskService
	enabled atomic.Bool
}

func (s *switchableSQLTaskOwner) SQLTaskService() TaskService {
	if !s.enabled.Load() {
		return nil
	}
	return s.TaskService
}

func TestUnsupportedSQLTaskOwnerDoesNotStarveOtherTasks(t *testing.T) {
	store := NewMemTaskStorage()
	owner := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	service := &switchableSQLTaskOwner{TaskService: owner}
	runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1),
		WithRunnerFetchLimit(1), WithRunnerFetchInterval(time.Millisecond), WithRunnerFetchTimeout(time.Second)).(*taskRunner)
	t.Cleanup(func() { require.NoError(t, runner.Stop()) })
	mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1",
		Metadata: task.TaskMetadata{ID: "sql", Executor: task.TaskCode_SQLTask}})
	mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1",
		Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}})

	values, err := runner.doFetch(context.Background(), context.Background())
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "other", values[0].Metadata.ID)
	service.enabled.Store(true)
	values, err = runner.doFetch(context.Background(), context.Background())
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, "sql", values[0].Metadata.ID, "a restored owner can fetch SQL work")
	service.enabled.Store(false)

	done := make(chan struct{}, 1)
	runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error {
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})
	require.NoError(t, runner.Start())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the unrelated task did not execute behind an unsupported SQL firing")
	}
}

type blockingFetchService struct {
	TaskService
	entered chan context.Context
	release chan struct{}
}

func (s *blockingFetchService) QueryAsyncTask(ctx context.Context, _ ...Condition) ([]task.AsyncTask, error) {
	select {
	case s.entered <- ctx:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		return nil, nil
	}
}

func TestTaskRunnerStopCancelsFetch(t *testing.T) {
	for _, exceptional := range []bool{false, true} {
		t.Run(fmt.Sprintf("exceptional=%t", exceptional), func(t *testing.T) {
			store := NewMemTaskStorage()
			owner := NewTaskService(runtime.DefaultRuntime(), store)
			wrapped := &blockingFetchService{TaskService: owner, entered: make(chan context.Context, 1), release: make(chan struct{})}
			runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerFetchInterval(time.Millisecond), WithRunnerFetchTimeout(time.Minute)).(*taskRunner)
			stopped := make(chan error, 1)
			stopping := false
			t.Cleanup(func() {
				close(wrapped.release)
				if stopping {
					require.NoError(t, <-stopped)
				} else {
					require.NoError(t, runner.Stop())
				}
				require.NoError(t, owner.Close())
			})
			mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "fetch", Executor: task.TaskCode_TestOnly}})
			if exceptional {
				runner.sqlDeferred.Add(1)
			}
			initialDeferred := runner.sqlDeferred.Load()
			require.NoError(t, runner.Start())
			var queryCtx context.Context
			select {
			case queryCtx = <-wrapped.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("fetch query did not start")
			}
			stopping = true
			go func() { stopped <- runner.Stop() }()
			select {
			case <-queryCtx.Done():
				require.ErrorIs(t, queryCtx.Err(), context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("runner stop did not cancel its in-flight query")
			}
			select {
			case err := <-stopped:
				stopping = false
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not stop after canceling its query")
			}
			require.Equal(t, initialDeferred, runner.sqlDeferred.Load(), "runner shutdown must not be recorded as a fetch failure")
		})
	}
}

func TestTaskRunnerFetchContextAndTimeout(t *testing.T) {
	owner := NewTaskService(runtime.DefaultRuntime(), NewMemTaskStorage())
	wrapped := &blockingFetchService{TaskService: owner, entered: make(chan context.Context, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(wrapped.release); require.NoError(t, owner.Close()) })
	runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerFetchTimeout(10*time.Millisecond)).(*taskRunner)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runner.doFetch(ctx, ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, wrapped.entered, "an already stopped runner must not query storage")
	require.Zero(t, runner.sqlDeferred.Load())

	_, err = runner.doFetch(context.Background(), context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, moerr.CauseDoFetch)
	require.EqualValues(t, 1, runner.sqlDeferred.Load(), "a storage timeout still triggers recovery")
}

type restrictedSQLFetchService struct {
	TaskService
	id       uint64
	failNext atomic.Bool
}

func (s *restrictedSQLFetchService) QueryAsyncTask(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error) {
	if s.failNext.Swap(false) {
		return nil, errors.New("injected fast fetch failure")
	}
	return s.TaskService.QueryAsyncTask(ctx, append(conditions, WithTaskIDCond(EQ, s.id))...)
}

func TestSQLTaskFairFetchHonorsRestrictedWrapperIDs(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage()
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"hidden", "allowed", "later"} {
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: id, Executor: task.TaskCode_TestOnly}})
	}
	all := mustGetTestAsyncTask(t, store, 3)
	wrapped := &restrictedSQLFetchService{TaskService: service, id: all[1].ID}
	runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(8), WithRunnerFetchInterval(time.Hour)).(*taskRunner)
	t.Cleanup(func() { require.NoError(t, runner.Stop()) })
	owner := service.(*taskService)
	high := all[2].ID
	runner.sqlFetch = sqlTaskFetchState{active: true, waveReady: true, waveHigh: high,
		other: sqlTaskFetchCursor{high: high, ready: true}, old: sqlTaskFetchCursor{high: high, ready: true}}
	wrapped.failNext.Store(true) // raw scan asks for each ID, while the wrapper selects only "allowed"
	values, err := runner.doSQLTaskFairFetch(ctx, owner)
	require.NoError(t, err)
	require.Len(t, values, 1)
	require.Equal(t, wrapped.id, values[0].ID)
	allowed := values[0]

	// A wrapper may replace the cursor predicate. Its already-seen ID must not
	// move the cursor backwards or be dispatched again on the fast path.
	runner.sqlFetch.nextSQL = false
	runner.sqlFetch.other = sqlTaskFetchCursor{after: wrapped.id, high: high, ready: true}
	values, err = runner.doSQLTaskFairFetch(ctx, owner)
	require.NoError(t, err)
	require.Empty(t, values)
	done := make(chan struct{}, 1)
	runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { done <- struct{}{}; return nil })
	require.True(t, runner.addToWait(ctx, allowed, false))
	require.NoError(t, runner.Start())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the allowed task did not execute")
	}
}

func TestSQLTaskFairFetchWrapperDoesNotWaitForHolderLock(t *testing.T) {
	for _, wrapStore := range []bool{false, true} {
		t.Run(fmt.Sprintf("storage_wrapper_%t", wrapStore), func(t *testing.T) {
			base := NewMemTaskStorage()
			mustAddTestAsyncTask(t, base, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}})
			firing := mustGetTestAsyncTask(t, base, 1)[0]
			holder := newRefreshableTaskStorage(runtime.DefaultRuntime(), func(context.Context, bool) (string, error) {
				return "address", nil
			}, NewFixedTaskStorageFactory(base)).(*refreshableTaskStorage)
			var store TaskStorage = holder
			if wrapStore {
				store = &transparentSQLFetchStorage{TaskStorage: holder}
			}
			service := NewTaskService(runtime.DefaultRuntime(), store)
			wrapped := &transparentSQLFetchService{TaskService: service}
			runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerFetchLimit(1), WithRunnerFetchInterval(5*time.Millisecond), WithRunnerFetchTimeout(100*time.Millisecond)).(*taskRunner)
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				require.NoError(t, service.Close())
			})
			runner.sqlFetch = sqlTaskFetchState{active: true, waveReady: true, waveHigh: firing.ID,
				other: sqlTaskFetchCursor{high: firing.ID, ready: true}, old: sqlTaskFetchCursor{high: firing.ID, ready: true}}
			done := make(chan error, 1)
			holder.mu.Lock()
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				_, err := runner.doSQLTaskFairFetch(ctx, service.(*taskService))
				done <- err
			}()
			var err error
			blocked := false
			select {
			case err = <-done:
			case <-time.After(500 * time.Millisecond):
				blocked = true
			}
			holder.mu.Unlock()
			require.False(t, blocked, "fair fetch waited on an unavailable holder past its deadline")
			require.ErrorIs(t, err, ErrNotReady)
			other := make(chan struct{}, 1)
			runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { other <- struct{}{}; return nil })
			require.NoError(t, runner.Start())
			select {
			case <-other:
			case <-time.After(5 * time.Second):
				t.Fatal("unrelated task did not progress after holder recovery")
			}
		})
	}
}

func TestSQLTaskDeferredParentDoesNotBlockQueuedTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemTaskStorage()
	service := NewTaskService(runtime.DefaultRuntime(), store)
	runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerMaxWaitTasks(140), WithRunnerFetchInterval(time.Hour)).(*taskRunner)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, runner.Stop())
		require.NoError(t, service.Close())
	})
	firstStarted, releaseFirst := make(chan struct{}), make(chan struct{})
	healthy, other := make(chan struct{}), make(chan struct{})
	var badCalls atomic.Int32
	runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, value task.Task) error {
		disposition := ctx.Value(sqlTaskDispositionContextKey{}).(*sqlTaskRunnerDisposition)
		disposition.handled = true
		if value.(*task.AsyncTask).ParentTaskID != "sql-task:1" {
			close(healthy)
			return nil
		}
		if badCalls.Add(1) == 1 {
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			<-ctx.Done() // a second failing admission would occupy the only slot
		}
		deferFailedSQLTask(ctx)
		return nil
	})
	runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { close(other); return nil })
	for id := uint64(1); id <= 128; id++ {
		require.True(t, runner.addToWait(ctx, task.AsyncTask{ID: id, ParentTaskID: "sql-task:1", Metadata: task.TaskMetadata{Executor: task.TaskCode_SQLTask}}, false))
	}
	require.True(t, runner.addToWait(ctx, task.AsyncTask{ID: 129, ParentTaskID: "sql-task:2", Metadata: task.TaskMetadata{Executor: task.TaskCode_SQLTask}}, false))
	require.True(t, runner.addToWait(ctx, task.AsyncTask{ID: 130, Metadata: task.TaskMetadata{Executor: task.TaskCode_TestOnly}}, false))
	require.NoError(t, runner.Start())
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first failing task did not start")
	}
	close(releaseFirst)
	for _, done := range []<-chan struct{}{healthy, other} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("a failed parent blocked an unrelated queued task")
		}
	}
	require.Equal(t, int32(1), badCalls.Load())
}

func TestSQLTaskDeferredParentDoesNotHideHealthyOldTask(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1)).(*taskRunner)
	for i := 0; i < 100; i++ {
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("bad-%d", i), Executor: task.TaskCode_SQLTask}})
	}
	mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:2", Metadata: task.TaskMetadata{ID: "healthy", Executor: task.TaskCode_SQLTask}})
	runner.recordDeferredSQLTaskParent("sql-task:1")
	found := false
	for i := 0; i < 6 && !found; i++ {
		values, err := runner.doFetch(context.Background(), context.Background())
		require.NoError(t, err)
		for _, value := range values {
			found = found || value.Metadata.ID == "healthy"
		}
	}
	require.True(t, found, "a failed parent must be excluded before the SQL fetch limit")
	values, err := store.QueryAsyncTask(context.Background(), WithTaskMetadataId(EQ, "healthy"))
	require.NoError(t, err)
	require.Len(t, values, 1)
	values[0].Status = task.TaskStatus_Completed
	_, err = store.UpdateAsyncTask(context.Background(), values, WithTaskIDCond(EQ, values[0].ID))
	require.NoError(t, err)
	parent := runner.sqlBackoff.Load()
	require.NotNil(t, parent)
	expired := &sqlTaskDeferredParent{id: parent.id, parent: parent.parent, retryAt: time.Now().Add(-time.Second), hardRetry: time.Now().Add(time.Second)}
	expired.oldChecked.Store(true)
	runner.sqlBackoff.Store(expired)
	found = false
	for i := 0; i < 8 && !found; i++ {
		values, err := runner.doFetch(context.Background(), context.Background())
		require.NoError(t, err)
		for _, value := range values {
			found = found || value.ParentTaskID == "sql-task:1"
		}
	}
	require.True(t, found, "the failed parent must be reconsidered after backoff expires")
}

func TestSQLTaskDeferredParentWaitsForActualOldLane(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1), WithRunnerFetchInterval(20*time.Second)).(*taskRunner)
	for _, value := range []task.AsyncTask{
		{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:1", Metadata: task.TaskMetadata{ID: "bad", Executor: task.TaskCode_SQLTask}},
		{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:2", Metadata: task.TaskMetadata{ID: "healthy", Executor: task.TaskCode_SQLTask}},
	} {
		mustAddTestAsyncTask(t, store, 1, value)
	}
	// Even if the retry time is reached before the next old lane, the marker
	// remains long enough to actually query that lane with the exclusion.
	marker := &sqlTaskDeferredParent{id: 1, parent: "sql-task:1", retryAt: time.Now().Add(-time.Second), hardRetry: time.Now().Add(time.Minute)}
	runner.sqlBackoff.Store(marker) // no generation change: marker alone activates fair fetching
	require.Same(t, marker, runner.deferredSQLTaskParent())
	found := false
	for i := 0; i < 6 && !found; i++ {
		values, err := runner.doFetch(context.Background(), context.Background())
		require.NoError(t, err)
		for _, value := range values {
			found = found || value.Metadata.ID == "healthy"
		}
	}
	require.True(t, found)
	require.True(t, marker.oldChecked.Load())
	require.Nil(t, runner.deferredSQLTaskParent())

	// An unusable exclusion query cannot suppress a parent forever.
	runner.sqlBackoff.Store(&sqlTaskDeferredParent{id: 1, parent: "sql-task:1", retryAt: time.Now().Add(-time.Minute), hardRetry: time.Now().Add(-time.Second)})
	require.Nil(t, runner.deferredSQLTaskParent())
}

func (s *failingSQLTaskFetchStore) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conds...)
	if _, hasID := (*c)[CondTaskID]; s.failNormal && !hasID {
		return nil, errors.New("injected malformed task in normal fetch")
	}
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok {
		if _, lane := (*c)[condSQLTaskLane]; lane && (id.op == GT || id.op == EQ && id.taskID == s.poison) {
			return nil, errors.New("injected selective fetch failure")
		}
	}
	return s.memTaskStorage.QueryAsyncTask(ctx, conds...)
}

func TestSQLTaskFairFetchKeepsOldNewAndOtherTasksMoving(t *testing.T) {
	ctx := context.Background()
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &failingSQLTaskFetchStore{memTaskStorage: base, failNormal: true}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1), WithRunnerParallelism(1), WithRunnerFetchTimeout(time.Second)).(*taskRunner)
	for i := 0; i < 35; i++ {
		mustAddTestAsyncTask(t, base, 1, task.AsyncTask{Status: task.TaskStatus_Completed, Metadata: task.TaskMetadata{ID: fmt.Sprintf("old-%d", i)}})
	}
	old := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "old-sql", Executor: task.TaskCode_SQLTask}}
	other := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}}
	poison := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "poison", Executor: task.TaskCode_SQLTask}}
	mustAddTestAsyncTask(t, base, 3, old, other, poison)
	all := mustGetTestAsyncTask(t, base, 38)
	store.poison = all[len(all)-1].ID
	_, err := runner.doFetch(context.Background(), context.Background())
	require.Error(t, err, "a malformed head row must activate exceptional fetching")
	_, err = runner.doFetch(context.Background(), context.Background()) // establishes the old high water
	require.NoError(t, err)
	fresh := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "new-sql", Executor: task.TaskCode_SQLTask}}
	mustAddTestAsyncTask(t, base, 1, fresh)
	seen := map[string]bool{}
	for i := 0; i < 20 && len(seen) < 3; i++ {
		values, err := runner.doFetch(context.Background(), context.Background())
		require.NoError(t, err)
		for _, value := range values {
			seen[value.Metadata.ID] = true
			value.Status = task.TaskStatus_Completed
			_, err = base.UpdateAsyncTask(ctx, []task.AsyncTask{value}, WithTaskIDCond(EQ, value.ID))
			require.NoError(t, err)
		}
	}
	require.True(t, seen["old-sql"], "the finite old snapshot must advance")
	require.True(t, seen["new-sql"], "new work must not wait for the old snapshot to drain")
	require.True(t, seen["other"], "non-SQL tasks must retain a separate opportunity")
}

func TestSQLTaskFairFetchRevisitsRejectedTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemTaskStorage()
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1), WithRunnerMaxWaitTasks(1)).(*taskRunner)
	value := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}}
	mustAddTestAsyncTask(t, store, 1, value)
	runner.sqlDeferred.Add(1)
	values, err := runner.doFetch(context.Background(), context.Background())
	require.NoError(t, err)
	require.Len(t, values, 1)
	blocker := task.AsyncTask{ID: 999}
	require.True(t, runner.addToWait(ctx, blocker, false))
	require.False(t, runner.addToWait(ctx, values[0], false))
	runner.sqlFetch.rewind(values[0].ID)
	rt := <-runner.waitTasksC
	runner.removeRunningTask(rt, nil)
	var found bool
	for i := 0; i < 4; i++ {
		values, err = runner.doFetch(context.Background(), context.Background())
		require.NoError(t, err)
		if len(values) != 0 && values[0].Metadata.ID == "other" {
			found = true
			break
		}
	}
	require.True(t, found, "a task rejected by a full queue must be revisited")
}

type interruptedSQLTaskFetchStore struct {
	*memTaskStorage
	reads  int
	cancel context.CancelFunc
}

type cancelPartialSQLTaskFetchStore struct {
	*memTaskStorage
	cancel context.CancelFunc
	lastID uint64
}

func (s *cancelPartialSQLTaskFetchStore) QueryAsyncTask(ctx context.Context, conditions ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conditions...)
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok {
		if id.op == GT {
			return nil, errors.New("use raw scan")
		}
		if id.op == EQ && id.taskID == s.lastID && s.cancel != nil {
			s.cancel()
			return nil, ctx.Err()
		}
	}
	return s.memTaskStorage.QueryAsyncTask(ctx, conditions...)
}

func TestSQLTaskFetchCancellationRevisitsPartialResults(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &cancelPartialSQLTaskFetchStore{memTaskStorage: base}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"first", "second", "third"} {
		mustAddTestAsyncTask(t, base, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: id, Executor: task.TaskCode_TestOnly}})
	}
	all := mustGetTestAsyncTask(t, base, 3)
	store.lastID = all[2].ID
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(3), WithRunnerFetchInterval(time.Millisecond)).(*taskRunner)
	runner.sqlFetch = sqlTaskFetchState{active: true, waveReady: true, waveHigh: store.lastID,
		other: sqlTaskFetchCursor{high: store.lastID, ready: true}, old: sqlTaskFetchCursor{high: store.lastID, ready: true}}
	ctx, cancel := context.WithCancel(context.Background())
	store.cancel = cancel
	done := make(chan struct{})
	go func() { defer close(done); runner.fetch(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled partial fetch did not stop")
	}
	require.Zero(t, len(runner.waitTasksC), "canceled results must not enter the wait queue")
	require.Equal(t, all[0].ID-1, runner.sqlFetch.other.after, "the first unqueued result must be revisited")

	store.cancel = nil
	runner.sqlFetch.nextSQL = false
	values, err := runner.doFetch(context.Background(), context.Background())
	require.NoError(t, err)
	require.Len(t, values, 3)
	for i := range all {
		require.Equal(t, all[i].ID, values[i].ID)
	}
}

func (s *interruptedSQLTaskFetchStore) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conds...)
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok && id.op == EQ {
		s.reads++
		if s.reads == 3 {
			s.cancel()
			return nil, ctx.Err()
		}
		values, err := s.memTaskStorage.QueryAsyncTask(ctx, conds...)
		if err == nil && len(values) == 1 && strings.HasPrefix(values[0].Metadata.ID, "filtered-") {
			return nil, nil // a decorator filters a row after its point lookup
		}
		return values, err
	}
	return s.memTaskStorage.QueryAsyncTask(ctx, conds...)
}

func TestSQLTaskRawFetchMakesProgressAcrossDeadlines(t *testing.T) {
	base := NewMemTaskStorage().(*memTaskStorage)
	store := &interruptedSQLTaskFetchStore{memTaskStorage: base}
	service := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(2)).(*taskRunner)
	for i := 0; i < 18; i++ {
		mustAddTestAsyncTask(t, base, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("filtered-%d", i), Executor: task.TaskCode_SQLTask}})
	}
	wanted := task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "wanted", Executor: task.TaskCode_SQLTask}}
	mustAddTestAsyncTask(t, base, 1, wanted)
	for i := 0; i < 3; i++ {
		mustAddTestAsyncTask(t, base, 1, task.AsyncTask{Status: task.TaskStatus_Completed, Metadata: task.TaskMetadata{ID: fmt.Sprintf("tail-%d", i)}})
	}
	cursor := sqlTaskFetchCursor{high: 22, ready: true}
	found := false
	interruptions := 0
	for i := 0; i < 15 && !found; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		store.reads, store.cancel = 0, cancel
		before := cursor.after
		values, err := runner.fetchSQLTaskRawPage(ctx, service.(*taskService), &cursor, true, 2, nil)
		cancel()
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
			interruptions++
		}
		require.Greater(t, cursor.after, before, "completed checks must survive a later timeout")
		for _, value := range values {
			found = found || value.Metadata.ID == "wanted"
		}
	}
	require.True(t, found, "repeated deadlines must not keep the cursor on the first page")
	require.Positive(t, interruptions, "the point-query cancellation path must actually execute")
}
