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

package taskservice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/stretchr/testify/require"
)

func TestScheduleSQLTaskCatchUpPersistsTriggerState(t *testing.T) {
	oldFetchInterval := fetchInterval
	fetchInterval = 100 * time.Millisecond
	t.Cleanup(func() {
		fetchInterval = oldFetchInterval
	})

	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := newTestSQLTask("task-scheduled", 1)
	sqlTask.CronExpr = "0 0 0 1 1 *"
	sqlTask.Timezone = "UTC"
	sqlTask.NextFireTime = time.Now().Add(-time.Minute).UnixMilli()
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-scheduled"))[0]

	ts.StartScheduleSQLTask()
	defer ts.StopScheduleSQLTask()

	waitHasTasks(t, store, 5*time.Second, WithTaskParentTaskIDCond(EQ, fmt.Sprintf("sql-task:%d", sqlTask.TaskID)))

	updated := mustGetTestSQLTask(t, store, 1, WithTaskIDCond(EQ, sqlTask.TaskID))[0]
	require.Equal(t, uint64(1), updated.TriggerCount)
	require.Greater(t, updated.NextFireTime, sqlTask.NextFireTime)

	runs, err := store.QueryAsyncTask(context.Background(), WithTaskParentTaskIDCond(EQ, fmt.Sprintf("sql-task:%d", sqlTask.TaskID)))
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, "sql-task:1:1", runs[0].Metadata.ID)
}

func TestScheduleSQLTaskSkipsManualOnlyTask(t *testing.T) {
	oldFetchInterval := fetchInterval
	fetchInterval = 100 * time.Millisecond
	t.Cleanup(func() {
		fetchInterval = oldFetchInterval
	})

	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	sqlTask := newTestSQLTask("task-manual", 1)
	sqlTask.CronExpr = ""
	sqlTask.NextFireTime = 0
	mustAddTestSQLTask(t, store, 1, sqlTask)

	ts.StartScheduleSQLTask()
	defer ts.StopScheduleSQLTask()

	time.Sleep(500 * time.Millisecond)
	tasks, err := store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)
}

func TestScheduleSQLTaskStartStopNoop(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	ts.StopScheduleSQLTask()
	ts.StartScheduleSQLTask()
	require.NotNil(t, ts.sqlCrons.stopper)
	require.NotNil(t, ts.sqlCrons.jobs)

	ts.StartScheduleSQLTask()
	ts.StopScheduleSQLTask()
	require.Nil(t, ts.sqlCrons.stopper)
	require.Nil(t, ts.sqlCrons.jobs)

	ts.StopScheduleSQLTask()
}

func TestLoadSQLTasksAddsReplacesAndRemoves(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		for id := range ts.sqlCrons.jobs {
			ts.removeSQLTask(id)
		}
		require.NoError(t, ts.Close())
	}()
	ts.sqlCrons.jobs = make(map[uint64]*sqlTaskCronJob)

	sqlTask := newTestSQLTask("task-refresh", 1)
	sqlTask.CronExpr = "0 0 0 1 1 *"
	sqlTask.Timezone = "UTC"
	sqlTask.NextFireTime = time.Now().Add(time.Hour).UnixMilli()
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-refresh"))[0]

	ts.loadSQLTasks(context.Background())
	require.Len(t, ts.sqlCrons.jobs, 1)
	require.Equal(t, sqlTask.CronExpr, ts.sqlCrons.jobs[sqlTask.TaskID].taskSnapshot().CronExpr)

	sqlTask.CronExpr = "0 */2 * * * *"
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	ts.loadSQLTasks(context.Background())
	require.Len(t, ts.sqlCrons.jobs, 1)
	require.Equal(t, "0 */2 * * * *", ts.sqlCrons.jobs[sqlTask.TaskID].taskSnapshot().CronExpr)

	sqlTask.Enabled = false
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	ts.loadSQLTasks(context.Background())
	require.Empty(t, ts.sqlCrons.jobs)
}

func TestSQLTaskCronJobEdges(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		require.NoError(t, ts.Close())
	}()

	_, err := newSQLTaskCronJob(SQLTask{CronExpr: "bad cron"}, ts)
	require.Error(t, err)

	oldTask := newTestSQLTask("task-old", 1)
	newTask := oldTask
	require.False(t, sqlTaskNeedsRefresh(oldTask, newTask))
	newTask.Timezone = "Asia/Shanghai"
	require.True(t, sqlTaskNeedsRefresh(oldTask, newTask))

	job := &sqlTaskCronJob{s: ts, task: SQLTask{TaskID: 999}}
	job.Run()

	sqlTask := newTestSQLTask("task-disabled", 1)
	sqlTask.Enabled = false
	sqlTask.CronExpr = "0 * * * * *"
	mustAddTestSQLTask(t, store, 1, sqlTask)
	sqlTask = mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "task-disabled"))[0]
	job.setTask(sqlTask)
	job.Run()
	tasks, err := store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)

	sqlTask.Enabled = true
	sqlTask.CronExpr = "bad cron"
	mustUpdateTestSQLTask(t, store, 1, []SQLTask{sqlTask}, WithTaskIDCond(EQ, sqlTask.TaskID))
	job.setTask(sqlTask)
	job.Run()
	tasks, err = store.QueryAsyncTask(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks)
}

func TestSQLTaskCronJobTaskSnapshotConcurrent(t *testing.T) {
	job := &sqlTaskCronJob{task: SQLTask{TaskID: 1}}
	const iterations = 1000

	var wg sync.WaitGroup
	badTaskID := make(chan uint64, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			job.setTask(SQLTask{TaskID: 1, TriggerCount: uint64(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			task := job.taskSnapshot()
			if task.TaskID != 1 {
				select {
				case badTaskID <- task.TaskID:
				default:
				}
			}
		}
	}()
	wg.Wait()
	select {
	case taskID := <-badTaskID:
		require.Equal(t, uint64(1), taskID)
	default:
	}
}

// Exercise the real refresh/removal path: the observer must see deletion only
// after reconciliation, never merely because storage has deleted the row.
func TestSQLTaskRefreshObserver(t *testing.T) {
	store := NewMemTaskStorage().(*memTaskStorage)
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() { require.NoError(t, ts.Close()) }()
	var snapshots [][]uint64
	restore := SetSQLTaskRefreshHookForTest(func(serviceID string, ids []uint64) {
		require.Equal(t, ts.rt.ServiceUUID(), serviceID)
		snapshots = append(snapshots, ids)
	}, nil)
	defer restore()
	sqlTask := newTestSQLTask("observer", 1)
	sqlTask.CronExpr = "0 0 0 1 1 *"
	sqlTask.Timezone = "UTC"
	sqlTask.NextFireTime = time.Now().Add(24 * time.Hour).UnixMilli()
	mustAddTestSQLTask(t, store, 1, sqlTask)
	created := mustGetTestSQLTask(t, store, 1, WithTaskName(EQ, "observer"))[0]
	// Drive refresh synchronously, avoiding a concurrent scheduler in this test.
	ts.sqlCrons.jobs = make(map[uint64]*sqlTaskCronJob)
	defer func() {
		for id := range ts.sqlCrons.jobs {
			ts.removeSQLTask(id)
		}
	}()
	ts.loadSQLTasks(context.Background())
	require.Equal(t, [][]uint64{{created.TaskID}}, snapshots)
	_, err := store.DeleteSQLTask(context.Background(), WithTaskIDCond(EQ, created.TaskID))
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	ts.loadSQLTasks(context.Background())
	require.Len(t, snapshots, 2)
	require.Empty(t, snapshots[1])
	require.Empty(t, ts.sqlCrons.jobs)
	require.Equal(t, []uint64{created.TaskID}, snapshots[0], "snapshots must be independently owned")
}

// The overdue path is managed by the stopper, not cron.Stop. Hold it across
// deletion and prove an empty refresh snapshot alone is insufficient.
func TestSQLTaskRefreshObserverTracksOverdueExecution(t *testing.T) {
	ctx := context.Background()
	mem := NewMemTaskStorage().(*memTaskStorage)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	store := &blockedSQLTaskObserverStorage{TaskStorage: mem, entered: make(chan struct{}), release: release}
	ts := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	ts.sqlCrons.stopper = stopper.NewStopper("observer-test")
	ts.sqlCrons.jobs = make(map[uint64]*sqlTaskCronJob)
	defer func() {
		unblock()
		ts.sqlCrons.stopper.Stop()
		for id := range ts.sqlCrons.jobs {
			ts.removeSQLTask(id)
		}
		require.NoError(t, ts.Close())
	}()
	var active atomic.Int64
	snapshots := make(chan []uint64, 2)
	restore := SetSQLTaskRefreshHookForTest(func(_ string, ids []uint64) { snapshots <- ids },
		func(_ string, _ uint64, started bool) {
			if started {
				active.Add(1)
			} else {
				active.Add(-1)
			}
		})
	defer restore()
	item := newTestSQLTask("overdue-observer", 1)
	item.CronExpr = "0 0 0 1 1 *"
	item.Timezone = "UTC"
	item.NextFireTime = time.Now().Add(-time.Minute).UnixMilli()
	mustAddTestSQLTask(t, mem, 1, item)
	created := mustGetTestSQLTask(t, mem, 1, WithTaskName(EQ, item.TaskName))[0]
	ts.loadSQLTasks(ctx)
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up did not enter storage")
	}
	require.Equal(t, []uint64{created.TaskID}, <-snapshots)
	require.Equal(t, int64(1), active.Load())
	_, err := mem.DeleteSQLTask(ctx, WithTaskIDCond(EQ, created.TaskID))
	require.NoError(t, err)
	ts.loadSQLTasks(ctx)
	require.Empty(t, <-snapshots)
	require.Equal(t, int64(1), active.Load(), "cache removal must not hide an in-flight catch-up")
	unblock()
	require.Eventually(t, func() bool { return active.Load() == 0 }, 5*time.Second, time.Millisecond)
}

type blockedSQLTaskObserverStorage struct {
	TaskStorage
	queries atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (s *blockedSQLTaskObserverStorage) QuerySQLTask(ctx context.Context, conds ...Condition) ([]SQLTask, error) {
	if s.queries.Add(1) == 2 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.TaskStorage.QuerySQLTask(ctx, conds...)
}
