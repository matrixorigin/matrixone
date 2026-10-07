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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		fetchInterval = 300 * time.Millisecond
		triggered := make(chan struct{}, 1)
		store.preUpdateCron = func() error {
			select {
			case triggered <- struct{}{}:
			default:
			}
			return nil
		}
		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "*/1 * * * * *"))

		s.StartScheduleCronTask()

		select {
		case <-triggered:
		case <-time.After(5 * time.Second):
			t.Fatal("cron scheduler did not trigger")
		}
		s.StopScheduleCronTask()
		tasks, err := store.QueryAsyncTask(ctx, WithTaskMetadataId(EQ, "t1:1"))
		require.NoError(t, err)
		require.Len(t, tasks, 1)
	})
}

func TestRetryScheduleCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		n := 0
		store.preUpdateCron = func() error {
			if n == 0 {
				n++
				return moerr.NewInfo(context.TODO(), "test error")
			}
			return nil
		}

		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "0 0 0 1 1 *"))

		cronTasks, err := s.QueryCronTask(ctx)
		require.NoError(t, err)
		require.Len(t, cronTasks, 1)
		job, err := newCronJob(cronTasks[0], s)
		require.NoError(t, err)

		job.doRunWithRetryBackoff(0)
		tasks, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, "t1"))
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.Equal(t, "t1:1", tasks[0].Metadata.ID)
		require.Equal(t, 1, n)
	})
}

// TestRetryScheduleCronTaskAllFailed tests the scenario where all retries fail.
// This simulates a persistent database failure where the cron task cannot be triggered
// even after all retry attempts.
func TestRetryScheduleCronTaskAllFailed(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		var failCount atomic.Int32
		store.preUpdateCron = func() error {
			failCount.Add(1)
			// Always return error to simulate persistent database failure
			return moerr.NewInfo(context.TODO(), "persistent database error")
		}

		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "0 0 0 1 1 *"))

		cronTasks, err := s.QueryCronTask(ctx)
		require.NoError(t, err)
		require.Len(t, cronTasks, 1)
		job, err := newCronJob(cronTasks[0], s)
		require.NoError(t, err)

		// Exercise one trigger synchronously so another scheduler tick cannot
		// race the exact retry-count assertion. Backoff duration is orthogonal
		// to retry behavior and is zero only for this focused test.
		job.doRunWithRetryBackoff(0)
		require.Equal(t, int32(cronTaskTriggerMaxRetries), failCount.Load())

		// Verify that no async task was created due to persistent failure
		tasks, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, "t1"))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(tasks), "no async task should be created when all retries fail")
	})
}

func TestScheduleCronTaskImmediately(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		task := newTestCronTask("t1", "0 0 0 1 1 *")
		task.CreateAt = time.Now().Add(-time.Second).UnixMilli()
		task.NextTime = task.CreateAt
		task.TriggerTimes = 0
		task.UpdateAt = time.Now().UnixMilli()

		mustAddTestCronTask(t, store, 1, task)
		updated := make(chan struct{}, 1)
		store.preUpdateCron = func() error {
			select {
			case updated <- struct{}{}:
			default:
			}
			return nil
		}

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()
		s.fetchCronTasksOnce(ctx)
		<-updated
		s.crons.stopForTest()

		tasks, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, "t1"))
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.Equal(t, "t1:1", tasks[0].Metadata.ID)
	})
}

func TestScheduleCronTaskLimitConcurrency(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		cronTask := newTestCronTask("t1", "0 0 0 1 1 *")
		cronTask.CreateAt = time.Now().UnixMilli()
		cronTask.NextTime = cronTask.CreateAt
		cronTask.TriggerTimes = 0
		cronTask.UpdateAt = time.Now().UnixMilli()
		cronTask.Metadata.Options.Concurrency = 1

		mustAddTestCronTask(t, store, 1, cronTask)
		firstRun := make(chan struct{}, 1)
		store.preUpdateCron = func() error {
			select {
			case firstRun <- struct{}{}:
			default:
			}
			return nil
		}

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()
		s.fetchCronTasksOnce(ctx)
		<-firstRun
		cronTasks, err := s.QueryCronTask(ctx)
		require.NoError(t, err)
		require.Len(t, cronTasks, 1)
		job := s.crons.jobs[cronTasks[0].ID]
		require.NotNil(t, job)
		s.crons.stopForTest()
		job.Run()

		tasks, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, "t1"))
		require.NoError(t, err)
		require.Len(t, tasks, 1)
		require.Equal(t, "t1:1", tasks[0].Metadata.ID)
	})
}

func TestRemovedCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "0 0 0 1 1 *"))

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()
		s.fetchCronTasksOnce(ctx)
		cronTasks, err := s.QueryCronTask(ctx)
		require.NoError(t, err)
		require.Len(t, cronTasks, 1)
		s.crons.jobs[cronTasks[0].ID].Run()
		s.crons.stopForTest()
		require.Len(t, s.crons.entries, 1)

		store.Lock()
		store.cronTaskIndexes = make(map[string]uint64)
		store.cronTasks = make(map[uint64]task.CronTask)
		store.Unlock()

		s.crons.startForTest()
		s.fetchCronTasksOnce(ctx)
		s.crons.stopForTest()
		require.Len(t, s.crons.entries, 0)
		require.Empty(t, s.crons.cron.Entries())
	})
}

func TestReplaceCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "0 0 0 1 1 *"))
		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()
		s.fetchCronTasksOnce(ctx)
		cronTasks, err := s.QueryCronTask(ctx)
		require.NoError(t, err)
		require.Len(t, cronTasks, 1)
		jobInCron := s.crons.jobs[cronTasks[0].ID]
		require.NotNil(t, jobInCron)
		s.crons.stopForTest()
		jobInCron.doRunWithRetryBackoff(0)

		taskInStore := store.cronTasks[jobInCron.task.ID]
		oldEntryID := s.crons.entries[jobInCron.task.ID]
		cronTaskID := jobInCron.task.ID

		require.Equal(t, jobInCron.task.TriggerTimes, taskInStore.TriggerTimes)
		require.Greater(t, taskInStore.TriggerTimes, uint64(0))
		firstTriggerTimes := taskInStore.TriggerTimes

		t.Log("set trigger times to 0")
		jobInCron.task.TriggerTimes = 0
		require.Equal(t, s.crons.jobs[cronTaskID].task.TriggerTimes, uint64(0))

		s.crons.startForTest()
		s.fetchCronTasksOnce(ctx)
		s.crons.stopForTest()
		require.Len(t, s.crons.entries, 1)
		require.NotEqual(t, oldEntryID, s.crons.entries[cronTaskID])

		entries := s.crons.cron.Entries()
		require.Len(t, entries, 1)
		require.Equal(t, s.crons.entries[cronTaskID], entries[0].ID)
		replacementJob, ok := entries[0].Job.(*cronJob)
		require.True(t, ok)
		taskInStore = store.cronTasks[replacementJob.task.ID]
		require.Equal(t, replacementJob.task.TriggerTimes, taskInStore.TriggerTimes)
		entries[0].WrappedJob.Run()
		require.Greater(t, store.cronTasks[cronTaskID].TriggerTimes, firstTriggerTimes)
	})
}

func (c *crons) stopForTest() {
	c.stopper.Stop()
	<-c.cron.Stop().Done()
}

func (c *crons) startForTest() {
	c.cron.Start()
	c.stopper = stopper.NewStopper("cronTasks")
}

func runScheduleCronTaskTest(t *testing.T, testFunc func(*memTaskStorage, *taskService, context.Context)) {
	oldFetchInterval := fetchInterval
	fetchInterval = time.Hour
	t.Cleanup(func() {
		fetchInterval = oldFetchInterval
	})

	store := NewMemTaskStorage().(*memTaskStorage)
	s := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	testFunc(store, s, ctx)
}

func waitHasTasks(t *testing.T, store *memTaskStorage, timeout time.Duration, conds ...Condition) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "wait any tasks failed")
			return
		default:
			tasks, err := store.QueryAsyncTask(ctx, conds...)
			require.NoError(t, err)
			if len(tasks) > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
}
