// Copyright 2021 - 2023 Matrix Origin
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type mockHAKeeperClientForDaemon struct {
	state logservicepb.CheckerState
	err   error
}

func (m *mockHAKeeperClientForDaemon) GetClusterDetails(context.Context) (logservicepb.ClusterDetails, error) {
	return logservicepb.ClusterDetails{}, nil
}

func (m *mockHAKeeperClientForDaemon) GetClusterState(context.Context) (logservicepb.CheckerState, error) {
	return m.state, m.err
}

type mockActiveRoutine struct {
	pauseC  chan struct{}
	resumeC chan struct{}
	cancelC chan struct{}
}

func newMockActiveRoutine() *mockActiveRoutine {
	return &mockActiveRoutine{
		pauseC:  make(chan struct{}, 1),
		resumeC: make(chan struct{}, 1),
		cancelC: make(chan struct{}, 1),
	}
}

func (r *mockActiveRoutine) Pause() error {
	r.pauseC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Resume() error {
	r.resumeC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Cancel() error {
	r.cancelC <- struct{}{}
	return nil
}

func (r *mockActiveRoutine) Restart() error {
	return nil
}

type mockErrActiveRoutine struct {
	pauseErr   error
	resumeErr  error
	cancelErr  error
	restartErr error
}

func (r *mockErrActiveRoutine) Pause() error   { return r.pauseErr }
func (r *mockErrActiveRoutine) Resume() error  { return r.resumeErr }
func (r *mockErrActiveRoutine) Cancel() error  { return r.cancelErr }
func (r *mockErrActiveRoutine) Restart() error { return r.restartErr }

type serviceWithDaemonHook struct {
	TaskService
	mu        sync.RWMutex
	queryErr  error
	updateErr error
}

func (s *serviceWithDaemonHook) QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error) {
	s.mu.RLock()
	queryErr := s.queryErr
	s.mu.RUnlock()
	if queryErr != nil {
		return nil, queryErr
	}
	return s.TaskService.QueryDaemonTask(ctx, conds...)
}

func (s *serviceWithDaemonHook) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
	s.mu.RLock()
	updateErr := s.updateErr
	s.mu.RUnlock()
	if updateErr != nil {
		return 0, updateErr
	}
	return s.TaskService.UpdateDaemonTask(ctx, tasks, conds...)
}

func (s *serviceWithDaemonHook) setQueryErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryErr = err
}

func (s *serviceWithDaemonHook) setUpdateErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateErr = err
}

func daemonTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "-",
		Executor: task.TaskCode_ConnectorKafkaSink,
		Options: task.TaskOptions{
			MaxRetryTimes: 0,
			RetryInterval: 0,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func newDaemonTaskForTest(id uint64, status task.TaskStatus, runner string) task.DaemonTask {
	nowTime := time.Now()
	t := task.DaemonTask{
		ID:         id,
		Metadata:   daemonTaskMetadata(),
		TaskStatus: status,
		TaskRunner: runner,
		CreateAt:   nowTime,
		UpdateAt:   nowTime,
		Details: &task.Details{
			AccountID: 0,
			Account:   "sys",
			Username:  "dump",
			Details: &task.Details_Connector{
				Connector: &task.ConnectorDetails{
					TableName: "d1.t1",
				},
			},
		},
	}
	return t
}

func newDaemonHandleTestRunner(t *testing.T) (*taskRunner, TaskStorage) {
	t.Helper()
	store := NewMemTaskStorage()
	s := NewTaskService(runtime.DefaultRuntime(), store)
	t.Cleanup(func() {
		require.NoError(t, s.Close())
	})
	r := NewTaskRunner("r1", s, func(string) bool { return true },
		WithRunnerLogger(logutil.GetPanicLoggerWithLevel(zap.DebugLevel)),
		WithRunnerFetchInterval(time.Millisecond)).(*taskRunner)
	return r, store
}

func TestStartTaskHandleBranches(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
	dt.Metadata.ID = "start-1"
	dt.LastHeartbeat = time.Now()
	mustAddTestDaemonTask(t, store, 1, dt)

	executed := atomic.Bool{}
	start := newStartTask(r, &daemonTask{
		task: dt,
		executor: func(context.Context, task.Task) error {
			executed.Store(true)
			return moerr.NewInternalErrorNoCtx("executor failed")
		},
	})

	// last heartbeat is not timeout, local runner should skip starting
	require.NoError(t, start.Handle(context.Background()))
	require.False(t, executed.Load())

	// force update error branch in startDaemonTask
	hook.setUpdateErr(moerr.NewInternalErrorNoCtx("update failed"))
	dt2 := newDaemonTaskForTest(2, task.TaskStatus_Created, "")
	dt2.Metadata.ID = "start-2"
	dt2.LastHeartbeat = time.Time{}
	mustAddTestDaemonTask(t, store, 1, dt2)
	start2 := newStartTask(r, &daemonTask{
		task: dt2,
		executor: func(context.Context, task.Task) error {
			return nil
		},
	})
	require.NoError(t, start2.Handle(context.Background()))
	hook.setUpdateErr(nil)

	// run executor and hit setDaemonTaskError branch
	dt3 := newDaemonTaskForTest(3, task.TaskStatus_Created, "")
	dt3.Metadata.ID = "start-3"
	mustAddTestDaemonTask(t, store, 1, dt3)
	start3 := newStartTask(r, &daemonTask{
		task: dt3,
		executor: func(context.Context, task.Task) error {
			executed.Store(true)
			return moerr.NewInternalErrorNoCtx("executor failed")
		},
	})
	require.NoError(t, start3.Handle(context.Background()))
	require.Eventually(t, func() bool {
		return executed.Load()
	}, time.Second, time.Millisecond*10)
}

func TestResumeTaskHandleBranchesDirect(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_ResumeRequested, r.runnerID)
	dt.Metadata.ID = "resume-1"
	mustAddTestDaemonTask(t, store, 1, dt)
	h := newResumeTask(r, &daemonTask{task: dt})

	hook.setQueryErr(moerr.NewInternalErrorNoCtx("query failed"))
	require.Error(t, h.Handle(context.Background()))
	hook.setQueryErr(nil)

	mustDeleteTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Error(t, h.Handle(context.Background()))
	mustAddTestDaemonTask(t, store, 1, dt)

	dt.TaskRunner = "r2"
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.Error(t, h.Handle(context.Background()))

	dt.TaskRunner = r.runnerID
	dt.TaskStatus = task.TaskStatus_Created
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, h.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_Running
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, h.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_ResumeRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	hook.setUpdateErr(moerr.NewInternalErrorNoCtx("update failed"))
	require.Error(t, h.Handle(context.Background()))
	hook.setUpdateErr(nil)

	require.Error(t, h.Handle(context.Background()))
}

func TestRestartTaskHandleBranchesDirect(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, r.runnerID)
	dt.Metadata.ID = "restart-1"
	mustAddTestDaemonTask(t, store, 1, dt)
	taskRef := &daemonTask{task: dt}
	h := newRestartTask(r, taskRef)

	hook.setQueryErr(moerr.NewInternalErrorNoCtx("query failed"))
	require.Error(t, h.Handle(context.Background()))
	hook.setQueryErr(nil)

	mustDeleteTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Error(t, h.Handle(context.Background()))
	mustAddTestDaemonTask(t, store, 1, dt)

	dt.TaskStatus = task.TaskStatus_Running
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, h.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_Created
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, h.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_RestartRequested
	dt.TaskRunner = "r2"
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.Error(t, h.Handle(context.Background()))

	dt.TaskRunner = r.runnerID
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	hook.setUpdateErr(moerr.NewInternalErrorNoCtx("update failed"))
	require.Error(t, h.Handle(context.Background()))
	hook.setUpdateErr(nil)

	require.Error(t, h.Handle(context.Background()))

	restartErr := moerr.NewInternalErrorNoCtx("restart failed")
	ar := ActiveRoutine(&mockErrActiveRoutine{restartErr: restartErr})
	taskRef.activeRoutine.Store(&ar)
	dt.TaskStatus = task.TaskStatus_RestartRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, h.Handle(context.Background()), "restart failed")
}

func TestPauseAndCancelTaskHandleBranchesDirect(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_PauseRequested, r.runnerID)
	dt.Metadata.ID = "pause-cancel-1"
	mustAddTestDaemonTask(t, store, 1, dt)
	taskRef := &daemonTask{task: dt}
	r.addDaemonTask(taskRef)

	pauseH := newPauseTask(r, taskRef)
	cancelH := newCancelTask(r, taskRef)

	hook.setQueryErr(moerr.NewInternalErrorNoCtx("query failed"))
	require.Error(t, pauseH.Handle(context.Background()))
	require.Error(t, cancelH.Handle(context.Background()))
	hook.setQueryErr(nil)

	mustDeleteTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Error(t, pauseH.Handle(context.Background()))
	require.Error(t, cancelH.Handle(context.Background()))
	mustAddTestDaemonTask(t, store, 1, dt)

	dt.TaskStatus = task.TaskStatus_Running
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, pauseH.Handle(context.Background()))
	require.NoError(t, cancelH.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_Paused
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, pauseH.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_Canceled
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.NoError(t, cancelH.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_PauseRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	hook.setUpdateErr(moerr.NewInternalErrorNoCtx("update failed"))
	require.Error(t, pauseH.Handle(context.Background()))
	hook.setUpdateErr(nil)
	require.Error(t, pauseH.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	hook.setUpdateErr(moerr.NewInternalErrorNoCtx("update failed"))
	require.Error(t, cancelH.Handle(context.Background()))
	hook.setUpdateErr(nil)
	require.Error(t, cancelH.Handle(context.Background()))

	ar1 := ActiveRoutine(&mockErrActiveRoutine{pauseErr: moerr.NewInternalErrorNoCtx("pause failed")})
	taskRef.activeRoutine.Store(&ar1)
	dt.TaskStatus = task.TaskStatus_PauseRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, pauseH.Handle(context.Background()), "pause failed")

	ar2 := ActiveRoutine(&mockErrActiveRoutine{cancelErr: moerr.NewInternalErrorNoCtx("cancel failed")})
	taskRef.activeRoutine.Store(&ar2)
	dt.TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, cancelH.Handle(context.Background()), "cancel failed")
}

func TestRunDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		r.RegisterExecutor(task.TaskCode_ConnectorKafkaSink, func(ctx context.Context, task task.Task) error {
			defer close(c)
			return nil
		})
		mustAddTestDaemonTask(t, store, 1, newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID))
		<-c
		tasks := mustGetTestDaemonTask(t, store, 1)
		assert.Equal(t, 1, len(tasks))
		tk := tasks[0]
		assert.Equal(t, task.TaskStatus_Running, tk.TaskStatus)
		assert.False(t, tk.CreateAt.IsZero())
		assert.False(t, tk.UpdateAt.IsZero())
		assert.Equal(t, r.runnerID, tk.TaskRunner)
		assert.False(t, tk.LastRun.IsZero())
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func (r *taskRunner) testRegisterExecutor(t *testing.T, code task.TaskCode, started *atomic.Bool) {
	r.RegisterExecutor(code, func(ctx context.Context, task task.Task) error {
		ar := newMockActiveRoutine()
		assert.NoError(t, r.Attach(context.Background(), 1, ar))
		started.Store(true)
		for {
			select {
			case <-ar.cancelC:
				return nil

			case <-ar.pauseC:
				select {
				case <-ctx.Done():
					return nil
				case <-ar.cancelC:
					return nil
				case <-ar.resumeC:
				}

			case <-ctx.Done():
				return nil
			}
		}
	})
}

func expectTaskStatus(
	t *testing.T, store TaskStorage, dt task.DaemonTask, before task.TaskStatus, after task.TaskStatus,
) {
	dt.TaskStatus = before
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
FOR:
	for {
		select {
		case <-timer.C:
			panic("daemon task update timeout")
		case <-ticker.C:
			tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, 1))
			assert.Equal(t, 1, len(tasks))
			tk := tasks[0]
			if tk.TaskStatus == after {
				break FOR
			}
		}
	}
}

func waitStarted(started *atomic.Bool, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			panic("start executor timeout")
		case <-ticker.C:
			if started.Load() {
				return
			}
		}
	}
}

func TestPauseResumeDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_PauseRequested, task.TaskStatus_Paused)
		expectTaskStatus(t, store, dt, task.TaskStatus_ResumeRequested, task.TaskStatus_Running)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestPauseTaskHandleIdempotent(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		localDT, ok := r.getDaemonTask(1)
		require.True(t, ok)

		h := newPauseTask(r, localDT)
		require.NoError(t, h.Handle(context.Background()))
		require.NoError(t, h.Handle(context.Background()))

		done := make(chan error, 1)
		go func() {
			done <- h.Handle(context.Background())
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("duplicate pause should not block")
		}
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestTaskNameFromDetails(t *testing.T) {
	require.Panics(t, func() {
		_ = taskNameFromDetails(task.DaemonTask{})
	})

	tk := task.DaemonTask{Details: &task.Details{}}
	require.Equal(t, "", taskNameFromDetails(tk))

	tk.Details = &task.Details{
		Details: &task.Details_Connector{
			Connector: &task.ConnectorDetails{TableName: "d1.t1"},
		},
	}
	require.Equal(t, "", taskNameFromDetails(tk))

	tk.Details = &task.Details{
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{TaskName: "cdc-task-1"},
		},
	}
	require.Equal(t, "cdc-task-1", taskNameFromDetails(tk))
}

func TestNewStartTaskHandleWithUnknownExecutor(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		// No executor registered for this task code; newDaemonTask should return an error.
		_, err := r.newDaemonTask(newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID))
		require.Error(t, err)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestSetDaemonTaskError(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		r.setDaemonTaskError(context.Background(), &daemonTask{task: dt}, moerr.NewInternalErrorNoCtx("mock daemon error"))
		tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, 1))
		require.Len(t, tasks, 1)
		require.Contains(t, tasks[0].Details.Error, "mock daemon error")
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestStartTasksWithNilHAKeeperClient(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		// Stop background workers first to avoid racing with poll goroutine
		// when overriding test-only runner fields.
		require.NoError(t, r.Stop())
		r.getClient = func() util.HAKeeperClient { return nil }
		tasks := r.startTasks(context.Background())
		require.Len(t, tasks, 1)
		require.Equal(t, uint64(1), tasks[0].ID)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestStartTasksWithHAKeeperClientState(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		// Stop background workers first to avoid racing with poll goroutine
		// when overriding test-only runner fields.
		require.NoError(t, r.Stop())
		r.cnUUID = "cn-1"
		r.getClient = func() util.HAKeeperClient {
			return &mockHAKeeperClientForDaemon{
				state: logservicepb.CheckerState{
					CNState: logservicepb.CNState{
						Stores: map[string]logservicepb.CNStoreInfo{
							"cn-1": {
								Labels: map[string]metadata.LabelList{
									"account": {Labels: []string{"sys"}},
								},
							},
						},
					},
				},
			}
		}
		tasks := r.startTasks(context.Background())
		require.NotEmpty(t, tasks)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestStartTasksWithHAKeeperClientError(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		// Stop background workers first to avoid racing with poll goroutine
		// when overriding test-only runner fields.
		require.NoError(t, r.Stop())
		r.getClient = func() util.HAKeeperClient {
			return &mockHAKeeperClientForDaemon{err: moerr.NewInternalErrorNoCtx("hakeeper unavailable")}
		}
		tasks := r.startTasks(context.Background())
		require.NotEmpty(t, tasks)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestDispatchTaskHandleCoverBranches(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		r.RegisterExecutor(task.TaskCode_ConnectorKafkaSink, func(context.Context, task.Task) error { return nil })

		t1 := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		t2 := newDaemonTaskForTest(2, task.TaskStatus_ResumeRequested, r.runnerID)
		t3 := newDaemonTaskForTest(3, task.TaskStatus_RestartRequested, r.runnerID)
		t4 := newDaemonTaskForTest(4, task.TaskStatus_PauseRequested, r.runnerID)
		t5 := newDaemonTaskForTest(5, task.TaskStatus_CancelRequested, r.runnerID)
		t1.Metadata.ID = "daemon-1"
		t2.Metadata.ID = "daemon-2"
		t3.Metadata.ID = "daemon-3"
		t4.Metadata.ID = "daemon-4"
		t5.Metadata.ID = "daemon-5"
		mustAddTestDaemonTask(t, store, 5, t1, t2, t3, t4, t5)

		r.daemonTasks.Lock()
		r.daemonTasks.m[2] = &daemonTask{task: t2}
		r.daemonTasks.m[4] = &daemonTask{task: t4}
		r.daemonTasks.Unlock()

		r.dispatchTaskHandle(context.Background())
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_CancelRequested, task.TaskStatus_Canceled)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRestartDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_RestartRequested, task.TaskStatus_Running)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

// TestRestartDaemonTaskWithEmptyRunner tests restart when TaskRunner is empty.
// This covers the bug fix where tasks with empty TaskRunner couldn't be restarted.
func TestRestartDaemonTaskWithEmptyRunner(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		// Create a task with empty TaskRunner (simulating newly created task)
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_ConnectorKafkaSink, &started)
		waitStarted(&started, time.Second*5)

		// Update task status to RestartRequested (TaskRunner still empty)
		dt.TaskStatus = task.TaskStatus_RestartRequested
		mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})

		// Wait for restart to complete
		expectTaskStatus(t, store, dt, task.TaskStatus_RestartRequested, task.TaskStatus_Running)

		// Verify TaskRunner was assigned
		updatedTasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, 1))
		assert.Len(t, updatedTasks, 1, "Should have exactly one task")
		assert.Equal(t, r.runnerID, updatedTasks[0].TaskRunner, "TaskRunner should be assigned to current runner")
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}
