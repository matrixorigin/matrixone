// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/stretchr/testify/require"
)

type schedulerLifecycleStorage struct {
	TaskStorage
	phase      string
	entered    chan context.Context
	release    chan struct{}
	active     atomic.Int32
	attempts   atomic.Int32
	sqlQueries atomic.Int32
}

func (s *schedulerLifecycleStorage) wait(ctx context.Context) error {
	s.active.Add(1)
	defer s.active.Add(-1)
	select {
	case s.entered <- ctx:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return errors.New("fixture force release")
	}
}
func (s *schedulerLifecycleStorage) Close() error {
	if s.active.Load() != 0 {
		return errors.New("storage closed before scheduler callback joined")
	}
	return s.TaskStorage.Close()
}
func (s *schedulerLifecycleStorage) QueryCronTask(ctx context.Context, conds ...Condition) ([]task.CronTask, error) {
	if s.phase == "query" && len(conds) > 0 {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}
	return s.TaskStorage.QueryCronTask(ctx, conds...)
}
func (s *schedulerLifecycleStorage) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	if s.phase == "concurrency" {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}
	return s.TaskStorage.QueryAsyncTask(ctx, conds...)
}
func (s *schedulerLifecycleStorage) UpdateCronTask(ctx context.Context, cronTask task.CronTask, asyncTask task.AsyncTask) (int, error) {
	s.attempts.Add(1)
	switch s.phase {
	case "update":
		return 0, s.wait(ctx)
	case "backoff":
		select {
		case s.entered <- ctx:
		default:
		}
		return 0, errors.New("transient storage failure")
	}
	n, err := s.TaskStorage.UpdateCronTask(ctx, cronTask, asyncTask)
	if s.phase == "commit" && err == nil {
		_ = s.wait(ctx)
	}
	return n, err
}
func (s *schedulerLifecycleStorage) QuerySQLTask(ctx context.Context, conds ...Condition) ([]SQLTask, error) {
	if s.sqlQueries.Add(1) == 2 && s.phase == "query" {
		if err := s.wait(ctx); err != nil {
			return nil, err
		}
	}
	return s.TaskStorage.QuerySQLTask(ctx, conds...)
}
func (s *schedulerLifecycleStorage) TriggerSQLTask(ctx context.Context, sqlTask SQLTask, asyncTask task.AsyncTask) (int, error) {
	s.attempts.Add(1)
	if s.phase == "trigger" {
		return 0, s.wait(ctx)
	}
	n, err := s.TaskStorage.TriggerSQLTask(ctx, sqlTask, asyncTask)
	if s.phase == "commit" && err == nil {
		_ = s.wait(ctx)
	}
	return n, err
}

// Discovery runs under the real stopper, then returns before callbacks finish.
// Capturing its query timeout instead of its lifetime also fails these tests.
func startLifecycleScheduler(t *testing.T, sqlTask bool, mode string, phase string) (*taskService, *memTaskStorage, *schedulerLifecycleStorage, func(), <-chan struct{}) {
	t.Helper()
	mem := NewMemTaskStorage().(*memTaskStorage)
	storage := &schedulerLifecycleStorage{TaskStorage: mem, phase: phase, entered: make(chan context.Context, 1), release: make(chan struct{})}
	s := NewTaskService(runtime.DefaultRuntime(), storage).(*taskService)
	var releaseOnce, closeOnce sync.Once
	closed := make(chan struct{})
	var closeErr error
	closeService := func() { closeOnce.Do(func() { go func() { closeErr = s.Close(); close(closed) }() }) }
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(storage.release) })
		closeService()
		select {
		case <-closed:
			require.NoError(t, closeErr)
		case <-time.After(10 * time.Second):
			t.Error("scheduler fixture did not join")
		}
	})
	next := time.Now().Add(-time.Minute).UnixMilli()
	expr := "0 0 0 1 1 *"
	if mode == "periodic" {
		next = time.Now().Add(time.Hour).UnixMilli()
		expr = "*/1 * * * * *"
	} else if mode == "future" {
		next = time.Now().Add(time.Hour).UnixMilli()
	}
	discovered := make(chan struct{})
	if sqlTask {
		v := newTestSQLTask("lifecycle", 1)
		v.CronExpr = expr
		v.Timezone = "UTC"
		v.NextFireTime = next
		mustAddTestSQLTask(t, mem, 1, v)
		s.StartScheduleSQLTask()
		require.NoError(t, s.sqlCrons.stopper.RunTask(func(ctx context.Context) { s.loadSQLTasks(ctx); close(discovered) }))
	} else {
		v := newTestCronTask("lifecycle", expr)
		v.NextTime = next
		if phase == "concurrency" {
			v.Metadata.Options.Concurrency = 1
		}
		mustAddTestCronTask(t, mem, 1, v)
		s.StartScheduleCronTask()
		require.NoError(t, s.crons.stopper.RunTask(func(ctx context.Context) { s.fetchCronTasksOnce(ctx); close(discovered) }))
	}
	select {
	case <-discovered:
	case <-time.After(10 * time.Second):
		t.Fatal("owner discovery did not return")
	}
	return s, mem, storage, closeService, closed
}

func TestSchedulerCallbacksCancelWithOwner(t *testing.T) {
	old := fetchInterval
	fetchInterval = time.Hour
	t.Cleanup(func() { fetchInterval = old })
	for _, c := range []struct {
		name, phase, mode string
		sql               bool
	}{
		{"cron catch-up query", "query", "catch-up", false},
		{"cron periodic query", "query", "periodic", false},
		{"cron periodic concurrency", "concurrency", "periodic", false},
		{"cron catch-up update", "update", "catch-up", false},
		{"sql periodic query", "query", "periodic", true},
		{"sql catch-up trigger", "trigger", "catch-up", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, mem, storage, closeService, closed := startLifecycleScheduler(t, c.sql, c.mode, c.phase)
			var operationCtx context.Context
			select {
			case operationCtx = <-storage.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("callback did not enter storage")
			}
			require.NoError(t, operationCtx.Err(), "discovery's temporary query context must not own a callback")
			closeService()
			select {
			case <-operationCtx.Done():
				require.ErrorIs(t, operationCtx.Err(), context.Canceled, "retirement must cancel the operation before its own timeout")
			case <-time.After(10 * time.Second):
				t.Fatal("scheduler owner did not cancel the storage operation")
			}
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Fatal("scheduler did not join canceled callbacks")
			}
			require.Equal(t, int32(0), storage.active.Load())
			if c.phase == "update" || c.phase == "trigger" {
				require.Equal(t, int32(1), storage.attempts.Load(), "retirement must not retry")
			}
			values, err := mem.QueryAsyncTask(t.Context())
			require.NoError(t, err)
			require.Empty(t, values)
		})
	}
}

func TestCronCancellationInterruptsRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := NewMemTaskStorage().(*memTaskStorage)
		storage := &schedulerLifecycleStorage{TaskStorage: mem, phase: "backoff", entered: make(chan context.Context, 1)}
		s := NewTaskService(runtime.DefaultRuntime(), storage).(*taskService)
		v := newTestCronTask("backoff", "0 0 0 1 1 *")
		mustAddTestCronTask(t, mem, 1, v)
		values, err := mem.QueryCronTask(t.Context())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		job, err := newCronJob(ctx, values[0], s)
		require.NoError(t, err)
		done := make(chan struct{})
		go func() { job.Run(); close(done) }()
		<-storage.entered
		synctest.Wait()
		stoppedAt := time.Now()
		cancel()
		<-done
		require.Equal(t, stoppedAt, time.Now(), "canceling backoff must not wait for its timer")
		require.Equal(t, int32(1), storage.attempts.Load())
		values, err = mem.QueryCronTask(t.Context())
		require.NoError(t, err)
		require.Zero(t, values[0].TriggerTimes)
	})
}

func TestSchedulerKeepsSuccessfulCommitWhenOwnerCancels(t *testing.T) {
	old := fetchInterval
	fetchInterval = time.Hour
	t.Cleanup(func() { fetchInterval = old })
	for _, sqlTask := range []bool{false, true} {
		name := "cron"
		if sqlTask {
			name = "sql"
		}
		t.Run(name, func(t *testing.T) {
			s, mem, storage, closeService, closed := startLifecycleScheduler(t, sqlTask, "catch-up", "commit")
			var cronJob *cronJob
			var sqlJob *sqlTaskCronJob
			if sqlTask {
				for _, job := range s.sqlCrons.jobs {
					sqlJob = job
				}
			} else {
				for _, job := range s.crons.jobs {
					cronJob = job
				}
			}
			select {
			case <-storage.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("actual storage commit did not complete")
			}
			closeService()
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Fatal("successful callback did not join")
			}
			values, err := mem.QueryAsyncTask(t.Context())
			require.NoError(t, err)
			require.Len(t, values, 1)
			require.Equal(t, int32(1), storage.attempts.Load())
			if sqlTask {
				require.Equal(t, uint64(1), sqlJob.taskSnapshot().TriggerCount)
			} else {
				require.Equal(t, uint64(1), cronJob.task.TriggerTimes)
			}
		})
	}
}

func TestSchedulerRestartDoesNotReviveOldCallbacks(t *testing.T) {
	old := fetchInterval
	fetchInterval = time.Hour
	t.Cleanup(func() { fetchInterval = old })
	for _, sqlTask := range []bool{false, true} {
		name := "cron"
		if sqlTask {
			name = "sql"
		}
		t.Run(name, func(t *testing.T) {
			s, mem, _, _, _ := startLifecycleScheduler(t, sqlTask, "future", "none")
			discovered := make(chan struct{})
			if sqlTask {
				var oldJob *sqlTaskCronJob
				for _, job := range s.sqlCrons.jobs {
					oldJob = job
				}
				before := oldJob.taskSnapshot().TriggerCount
				s.StopScheduleSQLTask()
				require.ErrorIs(t, oldJob.ctx.Err(), context.Canceled)
				s.StartScheduleSQLTask()
				require.NoError(t, s.sqlCrons.stopper.RunTask(func(ctx context.Context) { s.loadSQLTasks(ctx); close(discovered) }))
				select {
				case <-discovered:
				case <-time.After(10 * time.Second):
					t.Fatal("new SQL generation did not discover tasks")
				}
				oldJob.Run()
				require.Equal(t, before, oldJob.taskSnapshot().TriggerCount)
				fresh := s.sqlCrons.jobs[oldJob.taskSnapshot().TaskID]
				require.NoError(t, fresh.ctx.Err())
				fresh.Run()
				require.Equal(t, before+1, fresh.taskSnapshot().TriggerCount)
			} else {
				var oldJob *cronJob
				for _, job := range s.crons.jobs {
					oldJob = job
				}
				before := oldJob.task.TriggerTimes
				s.StopScheduleCronTask()
				require.ErrorIs(t, oldJob.ctx.Err(), context.Canceled)
				s.StartScheduleCronTask()
				require.NoError(t, s.crons.stopper.RunTask(func(ctx context.Context) { s.fetchCronTasksOnce(ctx); close(discovered) }))
				select {
				case <-discovered:
				case <-time.After(10 * time.Second):
					t.Fatal("new cron generation did not discover tasks")
				}
				oldJob.Run()
				require.Equal(t, before, oldJob.task.TriggerTimes)
				fresh := s.crons.jobs[oldJob.task.ID]
				require.NoError(t, fresh.ctx.Err())
				fresh.Run()
				require.Equal(t, before+1, fresh.task.TriggerTimes)
			}
			values, err := mem.QueryAsyncTask(t.Context())
			require.NoError(t, err)
			require.Len(t, values, 1)
		})
	}
}
