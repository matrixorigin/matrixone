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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "*/1 * * * * *"))

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()

		waitHasTasks(t, store, time.Second*20, WithTaskParentTaskIDCond(EQ, "t1"))
	}, time.Millisecond, time.Millisecond)

}

func TestRetryScheduleCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		n := 0
		store.preUpdateCron = func() error {
			if n == 0 {
				n++
				return moerr.NewInfo("test error")
			}
			return nil
		}

		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "*/1 * * * * *"))

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()

		waitHasTasks(t, store, time.Second*20, WithTaskParentTaskIDCond(EQ, "t1"))
	}, time.Millisecond, time.Millisecond)
}

func TestScheduleCronTaskImmediately(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		task := newTestCronTask("t1", "*/1 * * * * *")
		task.CreateAt = time.Now().UnixMilli()
		task.NextTime = task.CreateAt
		task.TriggerTimes = 0
		task.UpdateAt = time.Now().UnixMilli()

		mustAddTestCronTask(t, store, 1, task)

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()

		waitHasTasks(t, store, time.Second*20, WithTaskParentTaskIDCond(EQ, "t1"))
	}, time.Millisecond, time.Millisecond)
}

func TestRemovedCronTask(t *testing.T) {
	runScheduleCronTaskTest(t, func(store *memTaskStorage, s *taskService, ctx context.Context) {
		assert.NoError(t, s.CreateCronTask(ctx, newTestTaskMetadata("t1"), "*/1 * * * * *"))

		s.StartScheduleCronTask()
		defer s.StopScheduleCronTask()

		waitJobsCount(t, 1, s, time.Second*10)

		store.Lock()
		store.cronTaskIndexes = make(map[string]uint64)
		store.cronTasks = make(map[uint64]task.CronTask)
		store.Unlock()

		waitJobsCount(t, 0, s, time.Second*10)
	}, time.Millisecond, time.Millisecond)

}

func runScheduleCronTaskTest(t *testing.T,
	testFunc func(store *memTaskStorage, s *taskService, ctx context.Context),
	fetch, retry time.Duration) {
	retryInterval = retry
	fetchInterval = fetch

	store := NewMemTaskStorage().(*memTaskStorage)
	s := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
	defer func() {
		assert.NoError(t, s.Close())
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	testFunc(store, s, ctx)
}

func waitJobsCount(t *testing.T, n int, s *taskService, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			require.Fail(t, "wait jobs count failed")
			return
		default:
			s.crons.Lock()
			v := len(s.crons.jobs)
			s.crons.Unlock()

			if v == n {
				return
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
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
			tasks, err := store.Query(ctx, conds...)
			require.NoError(t, err)
			if len(tasks) > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
}
