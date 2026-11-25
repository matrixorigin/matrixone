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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type blockingSQLTaskAdmissionStore struct {
	TaskStorage
	first   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSQLTaskAdmissionStore) AcquireSQLTaskRun(ctx context.Context, def SQLTask, run SQLTaskRun, admission *SQLTaskRunAdmission) (uint64, error) {
	if s.first.CompareAndSwap(false, true) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return s.TaskStorage.AcquireSQLTaskRun(ctx, def, run, admission)
}

// A same-parent burst must be reconsidered after the first admission without
// waiting for another periodic fetch. The first admission is held explicitly.
func TestSQLTaskSameParentBurstMakesProgressBeforeNextPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		base := NewMemTaskStorage()
		store := &blockingSQLTaskAdmissionStore{TaskStorage: base, entered: make(chan struct{}), release: make(chan struct{})}
		service := NewTaskService(runtime.DefaultRuntime(), store)
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(3), WithRunnerFetchInterval(200*time.Millisecond)).(*taskRunner)
		var release sync.Once
		t.Cleanup(func() {
			release.Do(func() { close(store.release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, service.Close())
		})
		_, err := store.AddSQLTask(ctx, SQLTask{TaskName: "burst", AccountID: 1, Enabled: true, TriggerCount: 3, SQLBody: "select 1"})
		require.NoError(t, err)
		definitions, err := store.QuerySQLTask(ctx)
		require.NoError(t, err)
		require.Len(t, definitions, 1)
		def := definitions[0]
		parent := fmt.Sprintf("sql-task:%d", def.TaskID)
		for i := 1; i <= 3; i++ {
			spec := &task.SQLTaskContext{TaskId: def.TaskID, TaskName: def.TaskName, AccountId: def.AccountID, SQLBody: def.SQLBody,
				TriggerType: SQLTaskTriggerScheduled, TriggerCount: uint64(i)}
			_, err = store.AddAsyncTask(ctx, task.AsyncTask{Metadata: BuildSQLTaskMetadata(spec), ParentTaskID: parent,
				Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1})
			require.NoError(t, err)
		}
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, service, "cn1")
		runner.RegisterExecutor(task.TaskCode_SQLTask, executor.TaskExecutor())
		require.NoError(t, runner.Start())
		<-store.entered
		synctest.Wait()
		release.Do(func() { close(store.release) })
		synctest.Wait()
		firings, err := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, parent))
		require.NoError(t, err)
		require.Len(t, firings, 3)
		for _, firing := range firings {
			require.Equal(t, task.TaskStatus_Completed, firing.Status, "firing %s must be reconsidered before the next fetch tick", firing.Metadata.ID)
		}
	})
}

func TestSQLTaskGroupedPagesDoNotHideAnotherParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		base := NewMemTaskStorage()
		store := &blockingSQLTaskAdmissionStore{TaskStorage: base, entered: make(chan struct{}), release: make(chan struct{})}
		service := NewTaskService(runtime.DefaultRuntime(), store)
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(3), WithRunnerFetchLimit(32), WithRunnerFetchInterval(10*time.Millisecond)).(*taskRunner)
		var release sync.Once
		t.Cleanup(func() {
			release.Do(func() { close(store.release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, service.Close())
		})
		_, err := store.AddSQLTask(ctx,
			SQLTask{TaskName: "blocked", AccountID: 1, Enabled: true, TriggerCount: 1000, SQLBody: "select 1"},
			SQLTask{TaskName: "other", AccountID: 1, Enabled: true, TriggerCount: 1, SQLBody: "select 1"})
		require.NoError(t, err)
		defs, err := store.QuerySQLTask(ctx)
		require.NoError(t, err)
		require.Len(t, defs, 2)
		for _, def := range defs {
			count := 1
			if def.TaskName == "blocked" {
				count = 1000
			}
			parent := fmt.Sprintf("sql-task:%d", def.TaskID)
			for i := 1; i <= count; i++ {
				spec := &task.SQLTaskContext{TaskId: def.TaskID, TaskName: def.TaskName, AccountId: def.AccountID, SQLBody: def.SQLBody,
					TriggerType: SQLTaskTriggerScheduled, TriggerCount: uint64(i)}
				_, err = store.AddAsyncTask(ctx, task.AsyncTask{Metadata: BuildSQLTaskMetadata(spec), ParentTaskID: parent,
					Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1})
				require.NoError(t, err)
			}
		}
		otherParent := fmt.Sprintf("sql-task:%d", defs[1].TaskID)
		otherStarted := make(chan struct{}, 1)
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor { return &fakeInternalExecutor{} }, service, "cn1").TaskExecutor()
		runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, value task.Task) error {
			if value.(*task.AsyncTask).ParentTaskID == otherParent {
				otherStarted <- struct{}{}
			}
			return executor(ctx, value)
		})
		require.NoError(t, runner.Start())
		<-store.entered
		for i := 0; i < 160; i++ {
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			select {
			case <-otherStarted:
				return
			default:
			}
		}
		t.Fatal("grouped pages hid another parent behind a blocked parent")
	})
}

func TestSQLTaskLongBodyDoesNotDelayOverlapResolution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := NewMemTaskStorage()
		service := NewTaskService(runtime.DefaultRuntime(), store)
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(2), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, service.Close())
		})
		_, err := store.AddSQLTask(ctx, SQLTask{TaskName: "long", AccountID: 1, Enabled: true, TriggerCount: 2, SQLBody: "select 1"})
		require.NoError(t, err)
		defs, err := store.QuerySQLTask(ctx)
		require.NoError(t, err)
		require.Len(t, defs, 1)
		def := defs[0]
		parent := fmt.Sprintf("sql-task:%d", def.TaskID)
		for i := 1; i <= 2; i++ {
			spec := &task.SQLTaskContext{TaskId: def.TaskID, TaskName: def.TaskName, AccountId: def.AccountID, SQLBody: def.SQLBody,
				TriggerType: SQLTaskTriggerScheduled, TriggerCount: uint64(i)}
			_, err = store.AddAsyncTask(ctx, task.AsyncTask{Metadata: BuildSQLTaskMetadata(spec), ParentTaskID: parent,
				Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1})
			require.NoError(t, err)
		}
		entered := make(chan struct{})
		var calls atomic.Int32
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor {
			return &fakeInternalExecutor{execHook: func(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
				if calls.Add(1) == 1 {
					close(entered)
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
		}, service, "cn1")
		runner.RegisterExecutor(task.TaskCode_SQLTask, executor.TaskExecutor())
		require.NoError(t, runner.Start())
		<-entered
		for range 12 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			firings, queryErr := store.QueryAsyncTask(ctx, WithTaskParentTaskIDCond(EQ, parent))
			require.NoError(t, queryErr)
			if firings[1].Status == task.TaskStatus_Completed {
				require.Equal(t, task.TaskStatus_Running, firings[0].Status)
				require.EqualValues(t, 1, calls.Load(), "the overlapping firing must not run SQL")
				return
			}
		}
		t.Fatal("the overlapping firing did not resolve while the first SQL body was running")
	})
}
