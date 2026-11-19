// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package taskservice_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type externalTaskStorage struct {
	taskservice.TaskStorage
	unavailable atomic.Bool
	poison      string
	hidePrefix  string
}

func (s *externalTaskStorage) PublishSQLTaskReceipt(ctx context.Context, receipt taskservice.SQLTaskReceiptRequest) (bool, error) {
	if s.unavailable.Load() {
		return false, errors.New("injected metadata outage")
	}
	return s.TaskStorage.PublishSQLTaskReceipt(ctx, receipt)
}

func (s *externalTaskStorage) QueryAsyncTask(ctx context.Context, conditions ...taskservice.Condition) ([]task.AsyncTask, error) {
	values, err := s.TaskStorage.QueryAsyncTask(ctx, conditions...)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if s.poison != "" && value.Metadata.ID == s.poison {
			return nil, errors.New("injected malformed task options")
		}
	}
	if s.hidePrefix != "" {
		filtered := values[:0]
		for _, value := range values {
			if !strings.HasPrefix(value.Metadata.ID, s.hidePrefix) {
				filtered = append(filtered, value)
			}
		}
		values = filtered
	}
	return values, nil
}

type externalTaskService struct{ taskservice.TaskService }

type mismatchedTaskService struct {
	taskservice.TaskService
	store taskservice.TaskStorage
}

func (s *mismatchedTaskService) GetStorage() taskservice.TaskStorage { return s.store }

type noSQLTaskOwner struct{ taskservice.TaskService }

func (*noSQLTaskOwner) SQLTaskService() taskservice.TaskService { return nil }

type filteringTaskService struct {
	taskservice.TaskService
	calls    atomic.Int32
	failures int32
}

type observingUnclaimedStore struct {
	taskservice.TaskStorage
	finished            chan error
	failOnce            atomic.Bool
	commitBeforeFailure bool
}

func (s *observingUnclaimedStore) FinishSQLTaskUnclaimed(ctx context.Context, claim taskservice.SQLTaskClaim, result task.ExecuteResult, endAt int64) error {
	var err error
	if s.failOnce.CompareAndSwap(true, false) {
		if s.commitBeforeFailure {
			err = s.TaskStorage.FinishSQLTaskUnclaimed(ctx, claim, result, endAt)
		}
		if err == nil {
			err = errors.New("injected unclaimed completion outage")
		}
	} else {
		err = s.TaskStorage.FinishSQLTaskUnclaimed(ctx, claim, result, endAt)
	}
	select {
	case s.finished <- err:
	default:
	}
	return err
}

func (s *filteringTaskService) QueryAsyncTask(ctx context.Context, conditions ...taskservice.Condition) ([]task.AsyncTask, error) {
	if s.calls.Add(1) <= s.failures {
		return nil, errors.New("injected query failure")
	}
	values, err := s.TaskService.QueryAsyncTask(ctx, conditions...)
	if err != nil {
		return nil, err
	}
	filtered := values[:0]
	for _, value := range values {
		if value.Metadata.ID != "filtered" && !strings.HasPrefix(value.Metadata.ID, "hidden-") {
			filtered = append(filtered, value)
		}
	}
	return filtered, nil
}

type countingTaskExecutor struct {
	calls      atomic.Int32
	background atomic.Bool
}

func (e *countingTaskExecutor) Exec(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
	e.calls.Add(1)
	e.background.Store(ctx.Value(defines.SQLTaskBackgroundKey{}) == true)
	return nil
}

func (*countingTaskExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	panic("the test task has no gate")
}

func (*countingTaskExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func TestExternalTaskWrappersRecoverWithoutReplayingSQL(t *testing.T) {
	ctx := context.Background()
	store := &externalTaskStorage{TaskStorage: taskservice.NewMemTaskStorage()}
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	wrapped := &externalTaskService{TaskService: service}
	definition := taskservice.SQLTask{TaskName: "external", AccountID: 1, SQLBody: "insert into target values (1)", Enabled: true, TriggerCount: 1}
	n, err := store.AddSQLTask(ctx, definition)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	definitions, err := store.QuerySQLTask(ctx)
	require.NoError(t, err)
	require.Len(t, definitions, 1)
	definition = definitions[0]
	cn := strings.Repeat("界", 50) // valid VARCHAR(50), more than 128 UTF-8 bytes
	spec := &task.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
		SQLBody: definition.SQLBody, TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: 1}
	firing := task.AsyncTask{Metadata: taskservice.BuildSQLTaskMetadata(spec), ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
		Status: task.TaskStatus_Running, TaskRunner: cn, Epoch: 1}
	n, err = store.AddAsyncTask(ctx, firing)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	firings, err := store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firing.Metadata.ID))
	require.NoError(t, err)
	require.Len(t, firings, 1)
	store.unavailable.Store(true)
	fake := &countingTaskExecutor{}
	executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, wrapped, cn)
	err = executor.TaskExecutor()(ctx, &firings[0])
	var pending *taskservice.SQLTaskMetadataPendingError
	require.ErrorAs(t, err, &pending)
	require.True(t, pending.Recovering)
	require.Equal(t, int32(1), fake.calls.Load())
	require.True(t, fake.background.Load())
	store.unavailable.Store(false)
	require.Eventually(t, func() bool {
		runs, err := store.QuerySQLTaskRun(ctx)
		if err != nil || len(runs) != 1 || runs[0].Status != taskservice.SQLTaskStatusSuccess {
			return false
		}
		values, err := store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firing.Metadata.ID))
		return err == nil && len(values) == 1 && values[0].Status == task.TaskStatus_Completed
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(1), fake.calls.Load(), "metadata recovery must not execute SQL again")
	manual := *spec
	manual.TriggerType = taskservice.SQLTaskTriggerManual
	require.NoError(t, executor.ExecuteContext(ctx, &manual, true))
	require.Equal(t, int32(2), fake.calls.Load(), "the same wrapped service must also complete a healthy execution")
	require.False(t, fake.background.Load(), "manual SQL retains ordinary statement logging")
	page, err := store.ScanAsyncTaskPage(ctx, 0, ^uint64(0), false, 32)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, firings[0].ID, page[0].ID)
}

func TestExternalTaskServiceRejectsMissingOrMismatchedOwnerBeforeSQL(t *testing.T) {
	base := taskservice.NewMemTaskStorage()
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), base)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	other := taskservice.NewMemTaskStorage()
	t.Cleanup(func() { require.NoError(t, other.Close()) })
	var nilStore *externalTaskStorage
	nilStoreService := taskservice.NewTaskService(runtime.DefaultRuntime(), nilStore)
	t.Cleanup(func() { require.NoError(t, nilStoreService.Close()) })
	var nilWrapper *externalTaskService
	for _, wrapped := range []taskservice.TaskService{
		&noSQLTaskOwner{TaskService: service},
		&mismatchedTaskService{TaskService: service, store: other},
		nilStoreService,
		&externalTaskService{TaskService: nilStoreService},
		nilWrapper,
	} {
		fake := &countingTaskExecutor{}
		executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, wrapped, "cn")
		err := executor.ExecuteContext(context.Background(), &task.SQLTaskContext{TaskName: "x", TriggerType: taskservice.SQLTaskTriggerManual}, true)
		require.ErrorIs(t, err, taskservice.ErrNotReady)
		require.Zero(t, fake.calls.Load())
	}
}

func TestExternalTaskServiceFilterAppliesDuringFairFetch(t *testing.T) {
	for _, failures := range []int32{1, 2} { // fast query and raw-ID fallback
		t.Run(fmt.Sprintf("failures_%d", failures), func(t *testing.T) {
			store := taskservice.NewMemTaskStorage()
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			wrapped := &filteringTaskService{TaskService: service, failures: failures}
			runner := taskservice.NewTaskRunner("cn1", wrapped, nil, taskservice.WithRunnerParallelism(1), taskservice.WithRunnerFetchLimit(8), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				require.NoError(t, service.Close())
			})
			_, err := store.AddAsyncTask(context.Background(),
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "filtered", Executor: task.TaskCode_TestOnly}},
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "allowed", Executor: task.TaskCode_TestOnly}},
			)
			require.NoError(t, err)
			executed := make(chan string, 2)
			runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error {
				executed <- value.(*task.AsyncTask).Metadata.ID
				return nil
			})
			require.NoError(t, runner.Start())
			select {
			case id := <-executed:
				require.Equal(t, "allowed", id)
			case <-time.After(5 * time.Second):
				t.Fatal("the allowed task did not progress through fair fetch")
			}
			require.Greater(t, wrapped.calls.Load(), failures)
			select {
			case id := <-executed:
				require.NotEqual(t, "filtered", id)
			default:
			}
		})
	}
}

func TestExternalPostFilterDoesNotStarveFairFetch(t *testing.T) {
	for _, tc := range []struct {
		layer   string
		partial bool
	}{{"service", false}, {"service", true}, {"storage", false}, {"storage", true}} {
		t.Run(fmt.Sprintf("%s_partial_%t", tc.layer, tc.partial), func(t *testing.T) {
			ctx := context.Background()
			store := &externalTaskStorage{TaskStorage: taskservice.NewMemTaskStorage()}
			if tc.layer == "storage" {
				store.hidePrefix = "hidden-"
			}
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			var runnerService taskservice.TaskService = service
			if tc.layer == "service" {
				runnerService = &filteringTaskService{TaskService: service}
			}
			runner := taskservice.NewTaskRunner("cn1", runnerService, nil, taskservice.WithRunnerParallelism(2), taskservice.WithRunnerFetchLimit(64), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				store.unavailable.Store(false)
				require.NoError(t, service.Close())
			})
			definition := taskservice.SQLTask{TaskName: "pending", AccountID: 1, SQLBody: "select 1", Enabled: true}
			_, err := store.AddSQLTask(ctx, definition)
			require.NoError(t, err)
			definitions, err := store.QuerySQLTask(ctx)
			require.NoError(t, err)
			require.Len(t, definitions, 1)
			definition = definitions[0]
			store.unavailable.Store(true)
			fake := &countingTaskExecutor{}
			executor := taskservice.NewSQLTaskExecutor(func() ie.InternalExecutor { return fake }, service, "cn1")
			err = executor.ExecuteContext(ctx, &task.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName,
				AccountId: definition.AccountID, SQLBody: definition.SQLBody, TriggerType: taskservice.SQLTaskTriggerManual}, true)
			var pending *taskservice.SQLTaskMetadataPendingError
			require.ErrorAs(t, err, &pending)
			require.True(t, pending.Recovering)
			values := make([]task.AsyncTask, 0, 34)
			for i := 0; i < 32; i++ {
				id := fmt.Sprintf("hidden-%d", i)
				if tc.partial && i == 31 {
					id = "early-allowed"
				}
				values = append(values, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: id, Executor: task.TaskCode_TestOnly}})
			}
			values = append(values,
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "allowed", Executor: task.TaskCode_TestOnly}},
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1, ParentTaskID: "sql-task:99", Metadata: task.TaskMetadata{ID: "sql-old", Executor: task.TaskCode_SQLTask}},
			)
			_, err = store.AddAsyncTask(ctx, values...)
			require.NoError(t, err)
			normal, err := runnerService.QueryAsyncTask(ctx, taskservice.WithLimitCond(64))
			require.NoError(t, err)
			wanted := map[string]bool{"allowed": true, "sql-old": true}
			if tc.partial {
				wanted["early-allowed"] = true
			}
			require.Len(t, normal, len(wanted), "normal query can see tasks behind the filtered rows")
			done := make(chan string, 8)
			runner.RegisterExecutor(task.TaskCode_TestOnly, func(_ context.Context, value task.Task) error {
				done <- value.(*task.AsyncTask).Metadata.ID
				return nil
			})
			runner.RegisterExecutor(task.TaskCode_SQLTask, func(_ context.Context, value task.Task) error {
				done <- value.(*task.AsyncTask).Metadata.ID
				return errors.New("side-effect-free precheck")
			})
			require.NoError(t, runner.Start())
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			for len(wanted) > 0 {
				select {
				case id := <-done:
					require.True(t, wanted[id], "filtered or duplicate task: %s", id)
					delete(wanted, id)
				case <-deadline.C:
					t.Fatalf("post-LIMIT filter starved tasks during metadata recovery: %v", wanted)
				}
			}
			_, err = store.AddAsyncTask(ctx, task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1, ParentTaskID: "sql-task:100", Metadata: task.TaskMetadata{ID: "sql-new", Executor: task.TaskCode_SQLTask}})
			require.NoError(t, err)
			select {
			case id := <-done:
				require.Equal(t, "sql-new", id)
			case <-deadline.C:
				t.Fatal("a new SQL task did not progress during metadata recovery")
			}
			require.True(t, store.unavailable.Load(), "the task must progress before metadata recovers")
		})
	}
}

func TestExternalSQLExecutorEarlyReturnDoesNotCompleteClaimedFiring(t *testing.T) {
	for _, claimFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("claim_first_%t", claimFirst), func(t *testing.T) {
			ctx := context.Background()
			store := &observingUnclaimedStore{TaskStorage: taskservice.NewMemTaskStorage(), finished: make(chan error, 1)}
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			runner := taskservice.NewTaskRunner("cn1", service, nil, taskservice.WithRunnerParallelism(1), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				require.NoError(t, service.Close())
			})
			definition := taskservice.SQLTask{TaskName: "early", AccountID: 1, Enabled: true, TriggerCount: 1}
			_, err := store.AddSQLTask(ctx, definition)
			require.NoError(t, err)
			definitions, err := store.QuerySQLTask(ctx)
			require.NoError(t, err)
			require.Len(t, definitions, 1)
			definition = definitions[0]
			spec := &task.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
				TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: 1}
			firing := task.AsyncTask{Metadata: taskservice.BuildSQLTaskMetadata(spec), ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
				Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1}
			_, err = store.AddAsyncTask(ctx, firing)
			require.NoError(t, err)
			firings, err := store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firing.Metadata.ID))
			require.NoError(t, err)
			require.Len(t, firings, 1)
			firing = firings[0]
			started, release := make(chan struct{}), make(chan struct{})
			runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, _ task.Task) error {
				close(started)
				select {
				case <-release:
					return errors.New("precheck failed")
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			require.NoError(t, runner.Start())
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("SQL wrapper did not start")
			}
			if claimFirst {
				run := taskservice.SQLTaskRun{TaskID: definition.TaskID, TaskName: definition.TaskName, AccountID: definition.AccountID,
					Status: taskservice.SQLTaskStatusRunning, TriggerType: taskservice.SQLTaskTriggerScheduled, RunnerCN: "cn1", AttemptNumber: 1}
				admission := &taskservice.SQLTaskRunAdmission{AsyncTaskID: firing.ID, RunnerCN: firing.TaskRunner, Epoch: firing.Epoch,
					ParentTaskID: firing.ParentTaskID, MetadataID: firing.Metadata.ID, TriggerSequence: 1, Reserve: func(uint32, string) bool { return true }}
				id, err := store.AcquireSQLTaskRun(ctx, definition, run, admission)
				require.NoError(t, err)
				require.NotZero(t, id)
			}
			close(release)
			select {
			case err := <-store.finished:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("unclaimed completion did not run")
			}
			firings, err = store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firing.Metadata.ID))
			require.NoError(t, err)
			require.Len(t, firings, 1)
			if claimFirst {
				require.Equal(t, task.TaskStatus_Running, firings[0].Status)
				runs, err := store.QuerySQLTaskRun(ctx)
				require.NoError(t, err)
				require.Len(t, runs, 1)
				require.Equal(t, taskservice.SQLTaskStatusRunning, runs[0].Status)
			} else {
				require.Equal(t, task.TaskStatus_Completed, firings[0].Status)
				require.Equal(t, task.ResultCode_Failed, firings[0].ExecuteResult.Code)
				require.Contains(t, firings[0].ExecuteResult.Error, "precheck failed")
			}
		})
	}
}

func TestExternalSQLExecutorWrapperStopAndSelectedResult(t *testing.T) {
	precheck := errors.New("precheck failed")
	cases := []struct {
		name       string
		result     func(context.Context) error
		stopFirst  bool
		deferred   bool
		resultCode task.ResultCode
		errorText  string
	}{
		{"stop", func(ctx context.Context) error { return ctx.Err() }, true, true, 0, ""},
		{"wrapped_stop", func(ctx context.Context) error { return fmt.Errorf("wrapper stopped: %w", ctx.Err()) }, true, true, 0, ""},
		{"joined_stops", func(ctx context.Context) error { return errors.Join(ctx.Err(), fmt.Errorf("wrapper: %w", ctx.Err())) }, true, true, 0, ""},
		{"converted_stop", func(ctx context.Context) error { return moerr.ConvertGoError(context.Background(), ctx.Err()) }, true, true, 0, ""},
		{"selected_failure", func(context.Context) error { return precheck }, true, false, task.ResultCode_Failed, "precheck failed"},
		{"failure_joined_with_stop", func(ctx context.Context) error { return errors.Join(precheck, ctx.Err()) }, true, false, task.ResultCode_Failed, "precheck failed"},
		{"different_stop", func(context.Context) error { return context.DeadlineExceeded }, true, false, task.ResultCode_Failed, "deadline exceeded"},
		{"selected_success", func(context.Context) error { return nil }, true, false, task.ResultCode_Success, ""},
		{"live_context", func(context.Context) error { return context.Canceled }, false, false, task.ResultCode_Failed, "context canceled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &observingUnclaimedStore{TaskStorage: taskservice.NewMemTaskStorage(), finished: make(chan error, 1)}
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			runner := taskservice.NewTaskRunner("cn1", service, nil, taskservice.WithRunnerParallelism(1), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				require.NoError(t, service.Close())
			})
			_, err := store.AddSQLTask(ctx, taskservice.SQLTask{TaskName: "early", AccountID: 1, Enabled: true, TriggerCount: 1})
			require.NoError(t, err)
			definitions, err := store.QuerySQLTask(ctx)
			require.NoError(t, err)
			require.Len(t, definitions, 1)
			definition := definitions[0]
			spec := &task.SQLTaskContext{TaskId: definition.TaskID, TaskName: definition.TaskName, AccountId: definition.AccountID,
				TriggerType: taskservice.SQLTaskTriggerScheduled, TriggerCount: 1}
			firing := task.AsyncTask{Metadata: taskservice.BuildSQLTaskMetadata(spec), ParentTaskID: fmt.Sprintf("sql-task:%d", definition.TaskID),
				Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1}
			_, err = store.AddAsyncTask(ctx, firing)
			require.NoError(t, err)
			started := make(chan struct{})
			runner.RegisterExecutor(task.TaskCode_SQLTask, func(ctx context.Context, _ task.Task) error {
				close(started)
				if tc.stopFirst {
					<-ctx.Done()
				}
				return tc.result(ctx)
			})
			require.NoError(t, runner.Start())
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("SQL wrapper did not start")
			}
			if !tc.stopFirst {
				select {
				case err := <-store.finished:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("selected wrapper result was not completed")
				}
			}
			require.NoError(t, runner.Stop())
			if tc.stopFirst {
				select {
				case err := <-store.finished:
					require.False(t, tc.deferred, "stop-only result must not issue a completion CAS")
					require.NoError(t, err)
				default:
					require.True(t, tc.deferred, "selected result must issue a completion CAS")
				}
			}
			firings, err := store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, firing.Metadata.ID))
			require.NoError(t, err)
			require.Len(t, firings, 1)
			if tc.deferred {
				require.Equal(t, task.TaskStatus_Running, firings[0].Status)
				require.Nil(t, firings[0].ExecuteResult)
			} else {
				require.Equal(t, task.TaskStatus_Completed, firings[0].Status)
				require.NotNil(t, firings[0].ExecuteResult)
				require.Equal(t, tc.resultCode, firings[0].ExecuteResult.Code)
				require.Contains(t, firings[0].ExecuteResult.Error, tc.errorText)
			}
			runs, err := store.QuerySQLTaskRun(ctx)
			require.NoError(t, err)
			require.Empty(t, runs, "the wrapper did not admit or execute SQL")
		})
	}
}

func TestExternalSQLExecutorEarlyReturnRecoversUnclaimedCompletion(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_ack_%t", committed), func(t *testing.T) {
			ctx := context.Background()
			store := &observingUnclaimedStore{TaskStorage: taskservice.NewMemTaskStorage(), finished: make(chan error, 1), commitBeforeFailure: committed}
			store.failOnce.Store(true)
			service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
			runner := taskservice.NewTaskRunner("cn1", service, nil, taskservice.WithRunnerParallelism(1), taskservice.WithRunnerFetchLimit(1), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
			t.Cleanup(func() {
				require.NoError(t, runner.Stop())
				require.NoError(t, service.Close())
			})
			_, err := store.AddAsyncTask(ctx,
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Epoch: 1, ParentTaskID: "sql-task:1", Metadata: task.TaskMetadata{ID: "early", Executor: task.TaskCode_SQLTask}},
				task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "other", Executor: task.TaskCode_TestOnly}},
			)
			require.NoError(t, err)
			var prechecks atomic.Int32
			runner.RegisterExecutor(task.TaskCode_SQLTask, func(context.Context, task.Task) error {
				prechecks.Add(1) // side-effect-free precheck; no SQL body ran
				return errors.New("precheck failed")
			})
			other := make(chan struct{}, 1)
			runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { other <- struct{}{}; return nil })
			require.NoError(t, runner.Start())
			select {
			case <-other:
			case <-time.After(5 * time.Second):
				t.Fatal("an unclaimed completion outage blocked an unrelated task")
			}
			require.Eventually(t, func() bool {
				values, err := store.QueryAsyncTask(ctx, taskservice.WithTaskMetadataId(taskservice.EQ, "early"))
				return err == nil && len(values) == 1 && values[0].Status == task.TaskStatus_Completed &&
					values[0].ExecuteResult != nil && values[0].ExecuteResult.Code == task.ResultCode_Failed
			}, 5*time.Second, 10*time.Millisecond)
			if committed {
				require.Equal(t, int32(1), prechecks.Load(), "a committed result must not be reconsidered after a lost acknowledgement")
			} else {
				require.GreaterOrEqual(t, prechecks.Load(), int32(2), "an uncommitted precheck result must recover")
			}
		})
	}
}

func TestExternalTaskWrapperFetchesPastMalformedHead(t *testing.T) {
	store := &externalTaskStorage{TaskStorage: taskservice.NewMemTaskStorage(), poison: "malformed"}
	service := taskservice.NewTaskService(runtime.DefaultRuntime(), store)
	wrapped := &externalTaskService{TaskService: service}
	runner := taskservice.NewTaskRunner("cn1", wrapped, nil, taskservice.WithRunnerParallelism(1), taskservice.WithRunnerFetchLimit(1), taskservice.WithRunnerFetchInterval(5*time.Millisecond))
	t.Cleanup(func() {
		require.NoError(t, runner.Stop())
		require.NoError(t, service.Close())
	})
	_, err := store.AddAsyncTask(context.Background(),
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "malformed", Executor: task.TaskCode_SQLTask}},
		task.AsyncTask{Status: task.TaskStatus_Running, TaskRunner: "cn1", Metadata: task.TaskMetadata{ID: "healthy", Executor: task.TaskCode_TestOnly}},
	)
	require.NoError(t, err)
	done := make(chan struct{})
	var once sync.Once
	runner.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error {
		once.Do(func() { close(done) })
		return nil
	})
	require.NoError(t, runner.Start())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the undecodable head row blocked an unrelated task")
	}
}
