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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// This decorator narrows broad queries but honors an exact identity query. A
// full result does not prove that the omitted earlier row was inspected.
type narrowBroadTaskService struct {
	TaskService
	visible uint64
}

func (s *narrowBroadTaskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conds...)
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok && id.op == EQ {
		return s.TaskService.QueryAsyncTask(ctx, conds...)
	}
	return s.TaskService.QueryAsyncTask(ctx, append(conds, WithTaskIDCond(EQ, s.visible))...)
}

func TestSQLTaskFullDecoratorPageDoesNotHideExactCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 2,
			task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "hidden", Executor: task.TaskCode_TestOnly}},
			task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "visible", Executor: task.TaskCode_TestOnly}},
		)
		values := mustGetTestAsyncTask(t, store, 2)
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &narrowBroadTaskService{TaskService: owner, visible: values[1].ID}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, owner.Close())
		})
		started, hidden := make(chan struct{}), make(chan struct{}, 1)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error {
			if value.GetMetadata().ID == "visible" {
				close(started)
				<-release
			} else {
				hidden <- struct{}{}
			}
			return nil
		})
		require.NoError(t, runner.Start())
		<-started
		for range 30 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			select {
			case <-hidden:
				return
			default:
			}
		}
		t.Fatal("the full decorated page permanently hid an exact-ID candidate behind a locally running task")
	})
}

type laterTaskPagesService struct {
	TaskService
	hidden uint64
}

func (s *laterTaskPagesService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conds...)
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok && id.op == EQ {
		return s.TaskService.QueryAsyncTask(ctx, conds...)
	}
	return s.TaskService.QueryAsyncTask(ctx, append(conds, WithTaskIDCond(GT, s.hidden))...)
}

func TestSQLTaskContinuousFullDecoratorPagesReenterFairFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "hidden", Executor: task.TaskCode_TestOnly}})
		hidden := mustGetTestAsyncTask(t, store, 1)[0]
		for i := range 100 {
			mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("later-%d", i), Executor: task.TaskCode_TestOnly}})
		}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &laterTaskPagesService{TaskService: owner, hidden: hidden.ID}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(4), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		observed := make(chan string, 101)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error { observed <- value.GetMetadata().ID; return nil })
		require.NoError(t, runner.Start())
		for range 30 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			for len(observed) > 0 {
				if <-observed == "hidden" {
					return
				}
			}
		}
		t.Fatal("continuous full decorated pages hid the earlier exact-ID candidate")
	})
}

type broadPointTaskService struct{ TaskService }

func (s *broadPointTaskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	c := newConditions(conds...)
	if id, ok := (*c)[CondTaskID].(*taskIDCond); ok {
		if id.op == GT {
			return nil, errors.New("force raw-page fallback")
		}
		if id.op == EQ {
			conds = append(conds, WithTaskIDCond(GT, 0), WithLimitCond(2))
		}
	}
	return s.TaskService.QueryAsyncTask(ctx, conds...)
}

func TestSQLTaskPointQueryAcceptsExactIdentityAmongOtherRows(t *testing.T) {
	ctx := context.Background()
	store := NewMemTaskStorage()
	mustAddTestAsyncTask(t, store, 2,
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}},
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "target", Executor: task.TaskCode_TestOnly}},
	)
	all := mustGetTestAsyncTask(t, store, 2)
	owner := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	service := &broadPointTaskService{TaskService: owner}
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1)).(*taskRunner)
	cursor := sqlTaskFetchCursor{after: all[0].ID, high: all[1].ID, ready: true}
	values, err := runner.fetchSQLTaskRawPage(ctx, owner.(*taskService), &cursor, false, 1, nil)
	require.NoError(t, err)
	require.Len(t, values, 1, "an extra returned row must not hide the exact requested identity")
	require.Equal(t, all[1].ID, values[0].ID)
}

func TestSQLTaskPendingReceiptWithMultiplePointRowsProgresses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 2,
			task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "first", Executor: task.TaskCode_TestOnly}},
			task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "second", Executor: task.TaskCode_TestOnly}},
		)
		owner := NewTaskService(runtime.DefaultRuntime(), store).(*taskService)
		receiptEntered, releaseReceipt := make(chan struct{}), make(chan struct{})
		owner.sqlRecovery.process = func(ctx context.Context, _ *sqlTaskReceipt) bool {
			close(receiptEntered)
			select {
			case <-releaseReceipt:
				return true
			case <-ctx.Done():
				return false
			}
		}
		credit := owner.sqlRecovery.reserve(sqlTaskManualReceipt, 1, "blocked-receipt")
		require.NotNil(t, credit)
		require.True(t, owner.sqlRecovery.enqueue(&sqlTaskReceipt{SQLTaskReceiptRequest: SQLTaskReceiptRequest{Run: SQLTaskRun{RunID: 99}}, credit: credit}))
		<-receiptEntered
		wrapped := &broadPointTaskService{TaskService: owner}
		runner := NewTaskRunner("cn1", wrapped, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		releaseTasks := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(releaseTasks); close(releaseReceipt) }) }
		t.Cleanup(func() { release(); require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		started := make(chan string, 4)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(ctx context.Context, value task.Task) error {
			started <- value.GetMetadata().ID
			select {
			case <-releaseTasks:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		require.NoError(t, runner.Start())
		seen := map[string]int{}
		for range 30 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			for len(started) > 0 {
				seen[<-started]++
			}
			if len(seen) == 2 {
				require.Equal(t, 1, seen["first"])
				require.Equal(t, 1, seen["second"])
				return
			}
		}
		t.Fatalf("pending receipt and multi-row point results prevented progress: %v", seen)
	})
}

type overriddenConditionTaskService struct {
	TaskService
	conditions  []Condition
	unsupported bool
}

func (s *overriddenConditionTaskService) SQLTaskService() TaskService {
	if s.unsupported {
		return nil
	}
	return s.TaskService.SQLTaskService()
}

func (s *overriddenConditionTaskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	return s.TaskService.QueryAsyncTask(ctx, append(conds, s.conditions...)...)
}

func TestSQLTaskNormalQueryCannotExecuteWrongOwnerOrTerminalTask(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       task.AsyncTask
		conditions  []Condition
		unsupported bool
	}{
		{"other runner", task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn2", Metadata: task.TaskMetadata{ID: "wrong-runner", Executor: task.TaskCode_TestOnly}}, []Condition{WithTaskRunnerCond(EQ, "cn2")}, false},
		{"completed", task.AsyncTask{Status: task.TaskStatus_Completed, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "completed", Executor: task.TaskCode_TestOnly}}, []Condition{WithTaskStatusCond(task.TaskStatus_Completed)}, false},
		{"unsupported SQL owner", task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "sql", Executor: task.TaskCode_SQLTask}}, []Condition{withSQLTaskLane(true)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store := NewMemTaskStorage()
				mustAddTestAsyncTask(t, store, 1, tc.value)
				owner := NewTaskService(runtime.DefaultRuntime(), store)
				service := &overriddenConditionTaskService{TaskService: owner, conditions: tc.conditions, unsupported: tc.unsupported}
				runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
				t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
				executed := make(chan struct{}, 1)
				runner.RegisterExecutor(tc.value.Metadata.Executor, func(context.Context, task.Task) error { executed <- struct{}{}; return nil })
				require.NoError(t, runner.Start())
				for range 6 {
					time.Sleep(100 * time.Millisecond)
					synctest.Wait()
					select {
					case <-executed:
						t.Fatal("runner executed a task it does not own or that was already terminal")
					default:
					}
				}
			})
		})
	}
}

func TestSQLTaskWrongRunnerSQLFiringRemainsUnclaimed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := NewMemTaskStorage()
		_, err := store.AddSQLTask(ctx, SQLTask{TaskName: "remote", AccountID: 1, Enabled: true, TriggerCount: 1, SQLBody: "select 1"})
		require.NoError(t, err)
		defs, err := store.QuerySQLTask(ctx)
		require.NoError(t, err)
		require.Len(t, defs, 1)
		def := defs[0]
		spec := &task.SQLTaskContext{TaskId: def.TaskID, TaskName: def.TaskName, AccountId: def.AccountID, SQLBody: def.SQLBody, TriggerType: SQLTaskTriggerScheduled, TriggerCount: 1}
		_, err = store.AddAsyncTask(ctx, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn2", Epoch: 1, ParentTaskID: fmt.Sprintf("sql-task:%d", def.TaskID), Metadata: BuildSQLTaskMetadata(spec)})
		require.NoError(t, err)
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &overriddenConditionTaskService{TaskService: owner, conditions: []Condition{WithTaskRunnerCond(EQ, "cn2")}}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		var executions atomic.Int32
		executor := NewSQLTaskExecutor(func() ie.InternalExecutor { executions.Add(1); return &fakeInternalExecutor{} }, service, "cn1")
		runner.RegisterExecutor(task.TaskCode_SQLTask, executor.TaskExecutor())
		require.NoError(t, runner.Start())
		for range 6 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
		}
		firings, err := store.QueryAsyncTask(ctx)
		require.NoError(t, err)
		require.Len(t, firings, 1)
		require.Equal(t, task.TaskStatus_Running, firings[0].Status, "another CN's firing must not be completed")
		require.Zero(t, executions.Load(), "the wrong runner must not execute SQL")
		runs, err := store.QuerySQLTaskRun(ctx)
		require.NoError(t, err)
		require.Empty(t, runs)
	})
}

type duplicateTaskPageService struct{ TaskService }

func (s *duplicateTaskPageService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	values, err := s.TaskService.QueryAsyncTask(ctx, conds...)
	if err == nil && len(values) == 1 {
		values = append(values, values[0])
	}
	return values, err
}

func TestSQLTaskDuplicatePageDoesNotExecuteTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "same-task", Executor: task.TaskCode_TestOnly}})
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &duplicateTaskPageService{TaskService: owner}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(2), WithRunnerFetchLimit(2), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, owner.Close())
		})
		started := make(chan struct{}, 4)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { started <- struct{}{}; <-release; return nil })
		require.NoError(t, runner.Start())
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		require.Len(t, started, 1, "a repeated result ID must not start two executions")
	})
}

func TestSQLTaskCompletionBeforeDuplicateEnqueueIsNoop(t *testing.T) {
	owner := NewTaskService(runtime.DefaultRuntime(), NewMemTaskStorage())
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	runner := NewTaskRunner("cn1", owner, nil, WithRunnerMaxWaitTasks(2)).(*taskRunner)
	value := task.AsyncTask{ID: 1, Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "same-task", Executor: task.TaskCode_TestOnly}}
	require.True(t, runner.addToWait(context.Background(), value, false))
	first := <-runner.waitTasksC
	runner.removeRunningTask(first, nil) // completion races with a later copy in the same fetch
	require.True(t, runner.addToWait(context.Background(), value, false), "a duplicate is not queue saturation")
	require.Empty(t, runner.waitTasksC, "a recently completed task must not be re-enqueued")
}

type ownerlessTaskService struct{ TaskService }

func (s *ownerlessTaskService) SQLTaskService() TaskService { return nil }

func TestSQLTaskOwnerlessNormalPageKeepsConfiguredCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		for i := range 40 {
			mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("task-%d", i), Executor: task.TaskCode_TestOnly}})
		}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &ownerlessTaskService{TaskService: owner}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(40), WithRunnerFetchLimit(40), WithRunnerFetchInterval(100*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, owner.Close())
		})
		started := make(chan uint64, 40)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error { started <- value.GetID(); <-release; return nil })
		require.NoError(t, runner.Start())
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		require.Len(t, started, 40, "ownerless normal fetch must not strand capacity beyond 32")
	})
}

type lateOversizedTaskPageService struct{ TaskService }

func (s *lateOversizedTaskPageService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	values, err := s.TaskService.QueryAsyncTask(ctx, conds...)
	if err != nil || len(values) == 0 {
		return values, err
	}
	many := make([]task.AsyncTask, 1024)
	for i := range many {
		many[i] = values[0]
	}
	<-ctx.Done()
	return many, nil // misbehaving decorator returns success only after the fetch deadline
}

func TestSQLTaskExpiredOversizedNormalPageDoesNotDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "late", Executor: task.TaskCode_TestOnly}})
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &lateOversizedTaskPageService{TaskService: owner}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(1), WithRunnerFetchLimit(1), WithRunnerFetchInterval(100*time.Millisecond), WithRunnerFetchTimeout(10*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		executed := make(chan struct{}, 1024)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { executed <- struct{}{}; return nil })
		require.NoError(t, runner.Start())
		time.Sleep(120 * time.Millisecond)
		synctest.Wait()
		require.Empty(t, executed, "a page returned only after its fetch deadline must not start work")
	})
}

type cancelDuringPointInspection struct {
	context.Context
	cancel context.CancelFunc
	armed  atomic.Bool
	checks atomic.Int32
}

func (c *cancelDuringPointInspection) Err() error {
	if c.armed.Load() && c.checks.Add(1) == 3 {
		c.cancel()
	}
	return c.Context.Err()
}

type cancelPointTaskService struct {
	TaskService
	testCtx *cancelDuringPointInspection
}

func (s *cancelPointTaskService) QueryAsyncTask(ctx context.Context, conds ...Condition) ([]task.AsyncTask, error) {
	values, err := s.TaskService.QueryAsyncTask(ctx, append(conds, WithTaskIDCond(GT, 0), WithLimitCond(2))...)
	s.testCtx.armed.Store(true)
	return values, err
}

func TestSQLTaskCanceledPointInspectionDoesNotAdvance(t *testing.T) {
	store := NewMemTaskStorage()
	mustAddTestAsyncTask(t, store, 2,
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}},
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "target", Executor: task.TaskCode_TestOnly}},
	)
	all := mustGetTestAsyncTask(t, store, 2)
	owner := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelDuringPointInspection{Context: baseCtx, cancel: cancel}
	service := &cancelPointTaskService{TaskService: owner, testCtx: ctx}
	runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchLimit(1)).(*taskRunner)
	cursor := sqlTaskFetchCursor{after: all[0].ID, high: all[1].ID, ready: true}
	values, err := runner.fetchSQLTaskRawPage(ctx, owner.(*taskService), &cursor, false, 1, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, values)
	require.Equal(t, all[0].ID, cursor.after, "cancellation while inspecting returned rows must not skip the uninspected target")
}

type forwardTaskService struct{ TaskService }

func TestSQLTaskCappedNormalPageRevisitsRemainingCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		for i := range 33 {
			mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("task-%02d", i), Executor: task.TaskCode_TestOnly}})
		}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &forwardTaskService{TaskService: owner}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerParallelism(33), WithRunnerFetchLimit(64), WithRunnerFetchInterval(10*time.Millisecond)).(*taskRunner)
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() {
			once.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, owner.Close())
		})
		last := make(chan struct{}, 1)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error {
			if value.GetMetadata().ID == "task-32" {
				last <- struct{}{}
				return nil
			}
			<-release
			return nil
		})
		require.NoError(t, runner.Start())
		for range 30 {
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			select {
			case <-last:
				return
			default:
			}
		}
		t.Fatal("the uninspected candidate remained behind a capped normal page")
	})
}

func TestSQLTaskLiveFetchDeadlineTriggersRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "blocked", Executor: task.TaskCode_TestOnly}})
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		service := &blockingFetchService{TaskService: owner, entered: make(chan context.Context, 1), release: make(chan struct{})}
		runner := NewTaskRunner("cn1", service, nil, WithRunnerFetchInterval(100*time.Millisecond), WithRunnerFetchTimeout(10*time.Millisecond)).(*taskRunner)
		t.Cleanup(func() { close(service.release); require.NoError(t, runner.Stop()); require.NoError(t, owner.Close()) })
		require.NoError(t, runner.Start())
		queryCtx := <-service.entered
		<-queryCtx.Done()
		synctest.Wait()
		require.ErrorIs(t, queryCtx.Err(), context.DeadlineExceeded)
		require.ErrorIs(t, context.Cause(queryCtx), moerr.CauseDoFetch)
		require.Positive(t, runner.sqlDeferred.Load(), "a live-runner fetch timeout must arm recovery")
	})
}

func TestSQLTaskGroupedUntrustedPageArmsOneWave(t *testing.T) {
	store := NewMemTaskStorage()
	for i := range 2 {
		mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", ParentTaskID: "sql-task:1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("firing-%d", i), Executor: task.TaskCode_SQLTask}})
	}
	owner := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	runner := NewTaskRunner("cn1", &forwardTaskService{TaskService: owner}, nil, WithRunnerParallelism(2)).(*taskRunner)
	runner.fetchTasks(context.Background())
	require.EqualValues(t, 1, runner.sqlDeferred.Load(), "grouping must reuse the wave already armed by the untrusted page")
	require.Len(t, runner.waitTasksC, 1)
	runner.removeRunningTask(<-runner.waitTasksC, nil)
}

func TestSQLTaskTrustedFetchExpiryRevisitsUnqueuedCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemTaskStorage()
		for i := range 33 {
			mustAddTestAsyncTask(t, store, 1, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: fmt.Sprintf("wanted-%d", i), Executor: task.TaskCode_TestOnly}})
		}
		owner := NewTaskService(runtime.DefaultRuntime(), store)
		runner := NewTaskRunner("cn1", owner, nil, WithRunnerParallelism(33), WithRunnerFetchLimit(33), WithRunnerFetchInterval(100*time.Millisecond), WithRunnerFetchTimeout(10*time.Millisecond)).(*taskRunner)
		entered, release := make(chan struct{}), make(chan struct{})
		var logged atomic.Int32
		var enteredOnce, releaseOnce sync.Once
		core, logs := observer.New(zapcore.DebugLevel)
		runner.logger = zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
			if entry.Message == "new task fetched" && logged.Add(1) == 33 {
				enteredOnce.Do(func() { close(entered); <-release })
			}
			return nil
		}))
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(release) })
			require.NoError(t, runner.Stop())
			require.NoError(t, owner.Close())
		})
		executed := make(chan uint64, 64)
		runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error { executed <- value.GetID(); return nil })
		require.NoError(t, runner.Start())
		<-entered // the trusted query has returned, before its candidate is queued
		time.Sleep(20 * time.Millisecond)
		releaseOnce.Do(func() { close(release) })
		synctest.Wait()
		require.Empty(t, executed)
		require.EqualValues(t, 1, runner.sqlDeferred.Load(), "the live runner must revisit the interrupted candidate")
		entries := logs.FilterMessage("fetch task failed").All()
		require.Len(t, entries, 1)
		var fetchErr error
		for _, field := range entries[0].Context {
			if field.Key == "error" {
				fetchErr, _ = field.Interface.(error)
			}
		}
		require.ErrorIs(t, fetchErr, context.DeadlineExceeded)
		require.ErrorIs(t, fetchErr, moerr.CauseDoFetch)
		seen := make(map[uint64]bool)
		for range 20 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			for len(executed) != 0 {
				id := <-executed
				require.False(t, seen[id], "a revisited task must run once")
				seen[id] = true
			}
			if len(seen) == 33 {
				return
			}
		}
		t.Fatalf("fair recovery did not execute the full page: %d/33", len(seen))
	})
}
