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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

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

type mockFuncActiveRoutine struct {
	restart func() error
}

func (r *mockFuncActiveRoutine) Pause() error  { return nil }
func (r *mockFuncActiveRoutine) Resume() error { return nil }
func (r *mockFuncActiveRoutine) Cancel() error { return nil }
func (r *mockFuncActiveRoutine) Restart() error {
	return r.restart()
}

type serviceWithDaemonHook struct {
	TaskService
	mu         sync.RWMutex
	queryErr   error
	updateErr  error
	updateFn   func(context.Context, []task.DaemonTask, ...Condition) (int, error)
	queryCalls atomic.Int64
}

func (s *serviceWithDaemonHook) QueryDaemonTask(ctx context.Context, conds ...Condition) ([]task.DaemonTask, error) {
	s.queryCalls.Add(1)
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
	updateFn := s.updateFn
	s.mu.RUnlock()
	if updateFn != nil {
		return updateFn(ctx, tasks, conds...)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
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

func (s *serviceWithDaemonHook) setUpdateFn(
	fn func(context.Context, []task.DaemonTask, ...Condition) (int, error),
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateFn = fn
}

func TestDaemonTaskPollResumesAfterTaskFrameworkReenabled(t *testing.T) {
	wasDisabled := taskFrameworkDisabled()
	DebugCtlTaskFramework(true)
	t.Cleanup(func() {
		DebugCtlTaskFramework(wasDisabled)
	})

	r, _ := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	timerC := make(chan time.Time)
	resetC := make(chan struct{}, 2)
	go func() {
		defer close(done)
		r.pollWithTimer(ctx, timerC, func() {
			resetC <- struct{}{}
		})
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("daemon poll did not stop after cancellation")
		}
	}()

	timerC <- time.Now()
	select {
	case <-resetC:
	case <-time.After(time.Second):
		t.Fatal("disabled daemon poll did not reset its timer")
	}
	require.Zero(t, hook.queryCalls.Load())

	DebugCtlTaskFramework(false)
	timerC <- time.Now()
	select {
	case <-resetC:
	case <-time.After(time.Second):
		t.Fatal("enabled daemon poll did not reset its timer")
	}
	require.Positive(t, hook.queryCalls.Load())
}

func daemonTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "-",
		Executor: task.TaskCode_TestOnly,
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
			Details: &task.Details_ISCP{
				ISCP: &task.ISCPDetails{TaskName: "test-task"},
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

	// last heartbeat is not timeout, local runner should skip starting
	started, err := r.startDaemonTask(context.Background(), &daemonTask{task: dt}, false)
	require.NoError(t, err)
	require.False(t, started)

	// Force the admission update error deterministically. startTask.Handle
	// schedules work asynchronously, so wait for the injected call before
	// restoring the service hook.
	updateAttempted := make(chan struct{})
	hook.setUpdateFn(func(context.Context, []task.DaemonTask, ...Condition) (int, error) {
		close(updateAttempted)
		return 0, errors.New("update failed")
	})
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
	<-updateAttempted
	hook.setUpdateFn(nil)
	stored := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt2.ID))
	require.Equal(t, task.TaskStatus_Created, stored[0].TaskStatus)
	require.Empty(t, stored[0].Details.Error)

	// run executor and hit setDaemonTaskError branch
	errorPersisted := make(chan struct{})
	hook.setUpdateFn(func(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
		updated, err := hook.TaskService.UpdateDaemonTask(ctx, tasks, conds...)
		if err == nil && len(tasks) == 1 && tasks[0].Details.Error == "executor failed" {
			close(errorPersisted)
		}
		return updated, err
	})
	dt3 := newDaemonTaskForTest(3, task.TaskStatus_Created, "")
	dt3.Metadata.ID = "start-3"
	mustAddTestDaemonTask(t, store, 1, dt3)
	start3 := newStartTask(r, &daemonTask{
		task: dt3,
		executor: func(context.Context, task.Task) error {
			return errors.New("executor failed")
		},
	})
	require.NoError(t, start3.Handle(context.Background()))
	<-errorPersisted
	hook.setUpdateFn(nil)
	stored = mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt3.ID))
	require.Equal(t, task.TaskStatus_Running, stored[0].TaskStatus)
	require.Equal(t, "executor failed", stored[0].Details.Error)
}

func TestStartDaemonTaskPublishesClaimedSnapshot(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	dt := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
	mustAddTestDaemonTask(t, store, 1, dt)

	taskRef := &daemonTask{task: dt}
	started, err := r.startDaemonTask(context.Background(), taskRef, false)
	require.NoError(t, err)
	require.True(t, started)
	require.Equal(t, task.TaskStatus_Running, taskRef.task.TaskStatus)
	require.Equal(t, r.runnerID, taskRef.task.TaskRunner)
	require.False(t, taskRef.task.LastHeartbeat.IsZero())

	published, ok := r.getDaemonTask(dt.ID)
	require.True(t, ok)
	require.Same(t, taskRef, published)
}

func TestTaskRunnerStopJoinsExecutorReplacementTask(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	dt := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
	dt.Metadata.ID = "executor-replacement-owner"
	dt.Metadata.Executor = task.TaskCode_InitCdc
	mustAddTestDaemonTask(t, store, 1, dt)

	executorCtx := make(chan context.Context, 1)
	start := newStartTask(r, &daemonTask{
		task: dt,
		executor: func(ctx context.Context, _ task.Task) error {
			executorCtx <- ctx
			return nil
		},
	})
	require.NoError(t, start.Handle(context.Background()))

	var ctx context.Context
	select {
	case ctx = <-executorCtx:
	case <-time.After(time.Second):
		t.Fatal("task executor did not receive its runner-owned context")
	}
	scheduler := TaskExecutorTaskSchedulerFromContext(ctx)
	require.NotNil(t, scheduler)

	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	releaseCleanup := make(chan struct{})
	require.NoError(t, scheduler("replacement-cleanup", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(cancelObserved)
		<-releaseCleanup
	}))
	<-started

	// Stop normally runs only after Start. Marking the runner started here keeps
	// this focused test independent of its polling workers while exercising the
	// exact production Stop path and stopper ownership.
	r.started.Store(true)
	stopDone := make(chan error, 1)
	go func() { stopDone <- r.Stop() }()
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("task runner stop did not cancel the replacement")
	}
	select {
	case err := <-stopDone:
		t.Fatalf("task runner stop returned before replacement cleanup: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseCleanup)
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("task runner stop did not join replacement cleanup")
	}
}

func TestResumeTaskHandleBranchesDirect(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_ResumeRequested, r.runnerID)
	dt.Metadata.ID = "resume-1"
	mustAddTestDaemonTask(t, store, 1, dt)
	h := newResumeTask(r, &daemonTask{task: dt})

	hook.setQueryErr(errors.New("query failed"))
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
	hook.setUpdateErr(errors.New("update failed"))
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

	hook.setQueryErr(errors.New("query failed"))
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
	hook.setUpdateErr(errors.New("update failed"))
	require.Error(t, h.Handle(context.Background()))
	hook.setUpdateErr(nil)

	require.Error(t, h.Handle(context.Background()))

	restartErr := errors.New("restart failed")
	ar := ActiveRoutine(&mockErrActiveRoutine{restartErr: restartErr})
	taskRef.activeRoutine.Store(&ar)
	dt.TaskStatus = task.TaskStatus_RestartRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, h.Handle(context.Background()), "CDC restart failed")
	got, err := r.service.QueryDaemonTask(context.Background(), WithTaskIDCond(EQ, dt.ID))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, task.TaskStatus_RestartRequested, got[0].TaskStatus,
		"a failed replacement must remain retryable instead of being advertised as running")
}

func TestRestartTaskDoesNotOverwriteSupersedingControlRequest(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, r.runnerID)
	dt.Metadata.ID = "restart-cas"
	mustAddTestDaemonTask(t, store, 1, dt)

	taskRef := &daemonTask{task: dt}
	ar := ActiveRoutine(&mockFuncActiveRoutine{restart: func() error {
		current := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		require.Len(t, current, 1)
		current[0].TaskStatus = task.TaskStatus_CancelRequested
		mustUpdateTestDaemonTask(t, store, 1, current)
		return nil
	}})
	taskRef.activeRoutine.Store(&ar)

	require.NoError(t, newRestartTask(r, taskRef).Handle(context.Background()))
	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, got, 1)
	assert.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
}

func TestRestartTaskUsesFreshContextForStatusUpdate(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	hook := &serviceWithDaemonHook{TaskService: r.service}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, r.runnerID)
	dt.Metadata.ID = "restart-fresh-update-context"
	mustAddTestDaemonTask(t, store, 1, dt)

	ctx, cancel := context.WithCancel(context.Background())
	taskRef := &daemonTask{task: dt}
	ar := ActiveRoutine(&mockFuncActiveRoutine{restart: func() error {
		// Deterministically model the initial five-second handler budget
		// expiring while a valid two-phase restart is still completing.
		cancel()
		return nil
	}})
	taskRef.activeRoutine.Store(&ar)

	require.NoError(t, newRestartTask(r, taskRef).Handle(ctx))
	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, got, 1)
	assert.Equal(t, task.TaskStatus_Running, got[0].TaskStatus)
}

func TestRestartStartFailureReleasesClaimForRetry(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, "foreign-runner")
	dt.Metadata.ID = "restart-release-failed-claim"
	dt.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
	mustAddTestDaemonTask(t, store, 1, dt)

	startErr := errors.New("catalog transition unavailable")
	var restartAdmission atomic.Bool
	require.NoError(t, newRestartStartTask(r, &daemonTask{
		task: dt,
		executor: func(ctx context.Context, _ task.Task) error {
			restartAdmission.Store(IsRestartAdmission(ctx))
			return startErr
		},
	}).Handle(context.Background()))

	require.Eventually(t, func() bool {
		got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		return len(got) == 1 &&
			got[0].TaskStatus == task.TaskStatus_RestartRequested &&
			got[0].TaskRunner == "" &&
			got[0].LastHeartbeat.IsZero() &&
			got[0].Details.Error == "CDC restart startup failed"
	}, time.Second, time.Millisecond)
	require.True(t, restartAdmission.Load())
}

func TestRestartStartFailureDoesNotReleaseSupersedingControlRequest(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	claimed := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
	claimed.Metadata.ID = "restart-failed-claim-cas"
	mustAddTestDaemonTask(t, store, 1, claimed)

	superseding := claimed
	superseding.TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{superseding})

	r.releaseRestartClaim(
		&daemonTask{task: claimed},
		errors.New("catalog transition unavailable"),
	)
	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claimed.ID))
	require.Len(t, got, 1)
	require.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
	require.Equal(t, r.runnerID, got[0].TaskRunner)
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
	cancelH := newCancelTask(r, taskRef.task.ID)

	hook.setQueryErr(errors.New("query failed"))
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
	hook.setUpdateErr(errors.New("update failed"))
	require.Error(t, pauseH.Handle(context.Background()))
	hook.setUpdateErr(nil)
	require.Error(t, pauseH.Handle(context.Background()))

	dt.TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	cancelRoutine := newMockActiveRoutine()
	activeCancelRoutine := ActiveRoutine(cancelRoutine)
	taskRef.activeRoutine.Store(&activeCancelRoutine)
	hook.setUpdateErr(errors.New("update failed"))
	require.Error(t, cancelH.Handle(context.Background()))
	select {
	case <-cancelRoutine.cancelC:
		t.Fatal("failed status update invoked the active routine")
	default:
	}
	hook.setUpdateErr(nil)
	require.NoError(t, cancelH.Handle(context.Background()))
	select {
	case <-cancelRoutine.cancelC:
	default:
		t.Fatal("successful status update did not invoke the active routine")
	}

	ar1 := ActiveRoutine(&mockErrActiveRoutine{pauseErr: errors.New("pause failed")})
	taskRef.activeRoutine.Store(&ar1)
	dt.TaskStatus = task.TaskStatus_PauseRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, pauseH.Handle(context.Background()), "pause failed")

	ar2 := ActiveRoutine(&mockErrActiveRoutine{cancelErr: errors.New("cancel failed")})
	taskRef.activeRoutine.Store(&ar2)
	dt.TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{dt})
	require.ErrorContains(t, cancelH.Handle(context.Background()), "cancel failed")
}

func TestPauseTaskHandleCallsCompleteHookForNonLocalTask(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)

	completed := make(chan task.DaemonTask, 1)
	r.options.pauseTaskCompleted = func(ctx context.Context, tk task.DaemonTask) error {
		completed <- tk
		return nil
	}

	dt := newDaemonTaskForTest(1, task.TaskStatus_PauseRequested, "r2")
	dt.Metadata.ID = "pause-non-local-1"
	mustAddTestDaemonTask(t, store, 1, dt)

	pauseH := newPauseTask(r, &daemonTask{task: dt})
	require.NoError(t, pauseH.Handle(context.Background()))

	select {
	case tk := <-completed:
		require.Equal(t, dt.ID, tk.ID)
		require.Equal(t, task.TaskStatus_Paused, tk.TaskStatus)
	case <-time.After(time.Second):
		t.Fatal("pause complete hook was not called")
	}

	tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, tasks, 1)
	require.Equal(t, task.TaskStatus_Paused, tasks[0].TaskStatus)
}

func TestPauseTaskHandleKeepsPauseRequestedWhenActivePauseFails(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)

	dt := newDaemonTaskForTest(1, task.TaskStatus_PauseRequested, r.runnerID)
	dt.Metadata.ID = "pause-active-fail-1"
	mustAddTestDaemonTask(t, store, 1, dt)
	taskRef := &daemonTask{task: dt}
	r.addDaemonTask(taskRef)

	ar := ActiveRoutine(&mockErrActiveRoutine{pauseErr: errors.New("pause failed")})
	taskRef.activeRoutine.Store(&ar)

	pauseH := newPauseTask(r, taskRef)
	require.ErrorContains(t, pauseH.Handle(context.Background()), "pause failed")

	tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_PauseRequested, tasks[0].TaskStatus)
}

func TestPauseTasksRetriesPausedCDCFinalize(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)

	calls := atomic.Int32{}
	r.options.pauseTaskCompleted = func(ctx context.Context, tk task.DaemonTask) error {
		if calls.Add(1) == 1 {
			return errors.New("finalize failed")
		}
		return nil
	}

	dt := newDaemonTaskForTest(1, task.TaskStatus_PauseRequested, "r2")
	dt.Metadata.ID = "pause-finalize-retry-1"
	dt.Metadata.Executor = task.TaskCode_InitCdc
	dt.LastHeartbeat = time.Time{}
	mustAddTestDaemonTask(t, store, 1, dt)

	pauseH := newPauseTask(r, &daemonTask{task: dt})
	require.ErrorContains(t, pauseH.Handle(context.Background()), "finalize failed")
	require.Equal(t, int32(1), calls.Load())

	tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_Paused, tasks[0].TaskStatus)

	retryTasks := r.pauseTasks(context.Background())
	require.Len(t, retryTasks, 1)
	require.Equal(t, dt.ID, retryTasks[0].ID)
	require.Equal(t, task.TaskStatus_Paused, retryTasks[0].TaskStatus)

	retryH := newPauseTask(r, &daemonTask{task: retryTasks[0]})
	require.NoError(t, retryH.Handle(context.Background()))
	require.Equal(t, int32(2), calls.Load())
	require.Empty(t, r.pauseTasks(context.Background()))
}

func TestPauseCompletedTasksClearedByLifecycle(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)

	resumeDT := newDaemonTaskForTest(1, task.TaskStatus_ResumeRequested, r.runnerID)
	resumeDT.Metadata.ID = "pause-completed-resume-1"
	mustAddTestDaemonTask(t, store, 1, resumeDT)
	resumeRef := &daemonTask{task: resumeDT}
	resumeAR := ActiveRoutine(newMockActiveRoutine())
	resumeRef.activeRoutine.Store(&resumeAR)
	r.markPauseTaskCompleted(resumeDT.ID)
	require.NoError(t, newResumeTask(r, resumeRef).Handle(context.Background()))
	require.False(t, r.isPauseTaskCompleted(resumeDT.ID))

	cancelDT := newDaemonTaskForTest(2, task.TaskStatus_CancelRequested, r.runnerID)
	cancelDT.Metadata.ID = "pause-completed-cancel-1"
	mustAddTestDaemonTask(t, store, 1, cancelDT)
	r.markPauseTaskCompleted(cancelDT.ID)
	require.NoError(t, newCancelTask(r, cancelDT.ID).Handle(context.Background()))
	require.False(t, r.isPauseTaskCompleted(cancelDT.ID))

	removeDT := newDaemonTaskForTest(3, task.TaskStatus_Paused, r.runnerID)
	removeDT.Metadata.ID = "pause-completed-remove-1"
	r.addDaemonTask(&daemonTask{task: removeDT})
	r.markPauseTaskCompleted(removeDT.ID)
	r.removeDaemonTask(removeDT.ID)
	require.False(t, r.isPauseTaskCompleted(removeDT.ID))
}

func TestRunDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		c := make(chan struct{})
		r.RegisterExecutor(task.TaskCode_TestOnly, func(ctx context.Context, task task.Task) error {
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
	for _, code := range []task.TaskCode{
		task.TaskCode_TestOnly,
		task.TaskCode_ISCPExecutor,
		task.TaskCode_PublicationExecutor,
	} {
		t.Run(code.String(), func(t *testing.T) {
			runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
				dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
				dt.Metadata.Executor = code
				mustAddTestDaemonTask(t, store, 1, dt)
				var started atomic.Bool
				r.testRegisterExecutor(t, code, &started)
				waitStarted(&started, time.Second*5)

				expectTaskStatus(t, store, dt, task.TaskStatus_PauseRequested, task.TaskStatus_Paused)
				expectTaskStatus(t, store, dt, task.TaskStatus_ResumeRequested, task.TaskStatus_Running)
			}, WithRunnerParallelism(1),
				WithRunnerFetchInterval(time.Millisecond))
		})
	}
}

func TestPauseTaskHandleIdempotent(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_TestOnly, &started)
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
	require.Equal(t, "", taskNameFromDetails(task.DaemonTask{}))

	tk := task.DaemonTask{Details: &task.Details{}}
	require.Equal(t, "", taskNameFromDetails(tk))

	tk.Details = &task.Details{
		Details: &task.Details_ISCP{
			ISCP: &task.ISCPDetails{TaskName: "iscp-task"},
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
		// No executor registered for this task code; newStartTask should return safely.
		r.newStartTask(newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID))
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestSetDaemonTaskError(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		r.setDaemonTaskError(context.Background(), &daemonTask{task: dt}, errors.New("mock daemon error"))
		tasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, 1))
		require.Len(t, tasks, 1)
		require.Equal(t, "mock daemon error", tasks[0].Details.Error)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestSetDaemonTaskErrorDoesNotOverwriteSupersedingStateOrOwner(t *testing.T) {
	tests := []struct {
		name   string
		status task.TaskStatus
		runner string
	}{
		{"state", task.TaskStatus_CancelRequested, "r1"},
		{"owner", task.TaskStatus_Running, "r2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, store := newDaemonHandleTestRunner(t)
			claimed := newDaemonTaskForTest(1, task.TaskStatus_Running, r.runnerID)
			mustAddTestDaemonTask(t, store, 1, claimed)

			superseding := claimed
			superseding.TaskStatus = test.status
			superseding.TaskRunner = test.runner
			mustUpdateTestDaemonTask(t, store, 1, []task.DaemonTask{superseding})
			r.setDaemonTaskError(context.Background(), &daemonTask{task: claimed}, errors.New("late start error"))

			got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, claimed.ID))
			require.Equal(t, test.status, got[0].TaskStatus)
			require.Equal(t, test.runner, got[0].TaskRunner)
			require.Empty(t, got[0].Details.Error)
		})
	}
}

func TestStartTasksWithNilHAKeeperClient(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		r.hakeeper.Lock()
		r.hakeeper.getClient = func() util.HAKeeperClient { return nil }
		r.hakeeper.Unlock()
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

		r.hakeeper.Lock()
		r.hakeeper.cnUUID = "cn-1"
		r.hakeeper.getClient = func() util.HAKeeperClient {
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
		r.hakeeper.Unlock()
		tasks := r.startTasks(context.Background())
		require.NotEmpty(t, tasks)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestStartTasksWithHAKeeperClientError(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)

		r.hakeeper.Lock()
		r.hakeeper.getClient = func() util.HAKeeperClient {
			return &mockHAKeeperClientForDaemon{err: errors.New("hakeeper unavailable")}
		}
		r.hakeeper.Unlock()
		tasks := r.startTasks(context.Background())
		require.NotEmpty(t, tasks)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestDispatchTaskHandleCoverBranches(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		r.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { return nil })

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
		r.testRegisterExecutor(t, task.TaskCode_TestOnly, &started)
		waitStarted(&started, time.Second*5)

		expectTaskStatus(t, store, dt, task.TaskStatus_CancelRequested, task.TaskStatus_Canceled)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelDaemonTaskWithRemovedExecutor(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, _ TaskService, store TaskStorage) {
		require.Nil(t, r.GetExecutor(task.TaskCode(4)))
		legacyDetails := &task.Details{}
		require.NoError(t, legacyDetails.Unmarshal(
			[]byte{0x52, 0x06, 0x0a, 0x04, 'd', 'b', '.', 't'},
		))
		require.Nil(t, legacyDetails.Details)
		require.NotEmpty(t, legacyDetails.XXX_unrecognized)
		legacyWire := append([]byte(nil), legacyDetails.XXX_unrecognized...)

		// Model a rolling restart that kept the same CN UUID: the removed
		// executor cannot be in local admission even when the persisted owner and
		// heartbeat still look fresh.
		dt := newDaemonTaskForTest(1, task.TaskStatus_CancelRequested, r.runnerID)
		dt.LastHeartbeat = time.Now()
		dt.Metadata.Executor = task.TaskCode(4) // former ConnectorKafkaSink
		dt.Details = legacyDetails
		mustAddTestDaemonTask(t, store, 1, dt)

		expectTaskStatus(t, store, dt, task.TaskStatus_CancelRequested, task.TaskStatus_Canceled)
		got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		require.False(t, got[0].UpdateAt.IsZero())
		require.Equal(t, got[0].UpdateAt, got[0].EndAt)
		require.Equal(t, legacyWire, got[0].Details.XXX_unrecognized)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestCancelTaskWaitsForLocalRoutinePublication(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	r.RegisterExecutor(task.TaskCode_TestOnly, func(context.Context, task.Task) error { return nil })

	dt := newDaemonTaskForTest(1, task.TaskStatus_CancelRequested, r.runnerID)
	dt.LastHeartbeat = time.Now()
	mustAddTestDaemonTask(t, store, 1, dt)
	handler := newCancelTask(r, dt.ID)

	// Storage ownership is published before the local task map.
	require.NoError(t, handler.Handle(context.Background()))
	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)

	// The task map is published before the executor attaches its routine.
	taskRef := &daemonTask{task: dt}
	r.addDaemonTask(taskRef)
	require.NoError(t, handler.Handle(context.Background()))
	got = mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)

	// Once the routine is cancelable, the next poll completes the transition.
	routine := newMockActiveRoutine()
	require.NoError(t, r.Attach(context.Background(), dt.ID, routine))
	require.NoError(t, handler.Handle(context.Background()))
	got = mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_Canceled, got[0].TaskStatus)
	select {
	case <-routine.cancelC:
	default:
		t.Fatal("published active routine was not canceled")
	}
}

func TestCancelTaskDefersFreshForeignOwnerUntilStale(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	dt := newDaemonTaskForTest(1, task.TaskStatus_CancelRequested, "r2")
	dt.LastHeartbeat = time.Now()
	mustAddTestDaemonTask(t, store, 1, dt)

	require.NoError(t, newCancelTask(r, dt.ID).Handle(context.Background()))
	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
	require.Equal(t, "r2", got[0].TaskRunner)

	got[0].LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
	mustUpdateTestDaemonTask(t, store, 1, got)
	require.NoError(t, newCancelTask(r, dt.ID).Handle(context.Background()))
	got = mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Equal(t, task.TaskStatus_Canceled, got[0].TaskStatus)
}

func TestCancelTaskDoesNotOverwriteSupersedingStateOrOwner(t *testing.T) {
	tests := []struct {
		name   string
		status task.TaskStatus
		runner string
	}{
		{"state", task.TaskStatus_RestartRequested, "r1"},
		{"owner", task.TaskStatus_CancelRequested, "r2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, store := newDaemonHandleTestRunner(t)
			hook := &serviceWithDaemonHook{TaskService: r.service}
			r.service = hook

			dt := newDaemonTaskForTest(1, task.TaskStatus_CancelRequested, r.runnerID)
			mustAddTestDaemonTask(t, store, 1, dt)
			routine := newMockActiveRoutine()
			activeRoutine := ActiveRoutine(routine)
			taskRef := &daemonTask{task: dt}
			taskRef.activeRoutine.Store(&activeRoutine)
			r.addDaemonTask(taskRef)

			hook.setUpdateFn(func(ctx context.Context, tasks []task.DaemonTask, conds ...Condition) (int, error) {
				current := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
				current[0].TaskStatus = test.status
				current[0].TaskRunner = test.runner
				mustUpdateTestDaemonTask(t, store, 1, current)
				return hook.TaskService.UpdateDaemonTask(ctx, tasks, conds...)
			})

			require.NoError(t, newCancelTask(r, taskRef.task.ID).Handle(context.Background()))
			got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
			require.Equal(t, test.status, got[0].TaskStatus)
			require.Equal(t, test.runner, got[0].TaskRunner)
			select {
			case <-routine.cancelC:
				t.Fatal("stale cancellation invoked the active routine")
			default:
			}
		})
	}
}

func TestRestartDaemonTask(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_Created, r.runnerID)
		mustAddTestDaemonTask(t, store, 1, dt)
		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_TestOnly, &started)
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
		r.testRegisterExecutor(t, task.TaskCode_TestOnly, &started)
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

func TestRestartDaemonTaskTakesOverStaleForeignRunner(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, "foreign-runner")
		dt.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
		mustAddTestDaemonTask(t, store, 1, dt)

		var started atomic.Bool
		r.testRegisterExecutor(t, task.TaskCode_TestOnly, &started)
		expectTaskStatus(t, store, dt, task.TaskStatus_RestartRequested, task.TaskStatus_Running)

		updatedTasks := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		require.Len(t, updatedTasks, 1)
		assert.Equal(t, r.runnerID, updatedTasks[0].TaskRunner)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRestartDaemonTaskPassesRestartAdmissionToExecutor(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, "foreign-runner")
		dt.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
		mustAddTestDaemonTask(t, store, 1, dt)

		admission := make(chan bool, 1)
		r.RegisterExecutor(task.TaskCode_TestOnly, func(ctx context.Context, _ task.Task) error {
			admission <- IsRestartAdmission(ctx)
			return nil
		})

		expectTaskStatus(t, store, dt, task.TaskStatus_RestartRequested, task.TaskStatus_Running)
		select {
		case got := <-admission:
			require.True(t, got)
		case <-time.After(time.Second * 5):
			require.Fail(t, "restart executor was not invoked")
		}
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestRestartStartClaimDoesNotOverwriteSupersedingControlRequest(t *testing.T) {
	runTaskRunnerTest(t, func(r *taskRunner, s TaskService, store TaskStorage) {
		dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, "foreign-runner")
		dt.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
		mustAddTestDaemonTask(t, store, 1, dt)

		current := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		require.Len(t, current, 1)
		current[0].TaskStatus = task.TaskStatus_CancelRequested
		mustUpdateTestDaemonTask(t, store, 1, current)

		claimed, err := r.startDaemonTask(context.Background(), &daemonTask{task: dt}, true)
		require.NoError(t, err)
		assert.False(t, claimed)

		got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
		require.Len(t, got, 1)
		assert.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
	}, WithRunnerParallelism(1),
		WithRunnerFetchInterval(time.Millisecond))
}

func TestStartClaimDoesNotOverwriteSupersedingControlRequest(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)

	dt := newDaemonTaskForTest(1, task.TaskStatus_Created, "")
	dt.Metadata.ID = "start-claim-status-cas"
	mustAddTestDaemonTask(t, store, 1, dt)

	current := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, current, 1)
	current[0].TaskStatus = task.TaskStatus_CancelRequested
	mustUpdateTestDaemonTask(t, store, 1, current)

	claimed, err := r.startDaemonTask(context.Background(), &daemonTask{task: dt}, false)
	require.NoError(t, err)
	assert.False(t, claimed)

	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, got, 1)
	assert.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
}

func TestRestartStartClaimErrorPreservesSupersedingControlRequest(t *testing.T) {
	r, store := newDaemonHandleTestRunner(t)
	baseService := r.service
	hook := &serviceWithDaemonHook{TaskService: baseService}
	r.service = hook

	dt := newDaemonTaskForTest(1, task.TaskStatus_RestartRequested, "foreign-runner")
	dt.Metadata.ID = "restart-claim-error-cas"
	dt.LastHeartbeat = time.Now().Add(-r.options.heartbeatTimeout - time.Second)
	mustAddTestDaemonTask(t, store, 1, dt)

	claimStarted := make(chan struct{})
	releaseClaim := make(chan struct{})
	claimErr := errors.New("restart claim update failed")
	var updateCalls atomic.Int64
	hook.setUpdateFn(func(
		_ context.Context,
		tasks []task.DaemonTask,
		conds ...Condition,
	) (int, error) {
		if updateCalls.Add(1) != 1 {
			return baseService.UpdateDaemonTask(context.Background(), tasks, conds...)
		}
		close(claimStarted)
		<-releaseClaim

		superseding := dt
		superseding.TaskStatus = task.TaskStatus_CancelRequested
		updated, err := baseService.UpdateDaemonTask(
			context.Background(),
			[]task.DaemonTask{superseding},
		)
		if err != nil {
			return 0, err
		}
		if updated != 1 {
			return 0, errors.New("failed to install superseding control request")
		}
		return 0, claimErr
	})

	var executed atomic.Bool
	start := newRestartStartTask(r, &daemonTask{
		task: dt,
		executor: func(context.Context, task.Task) error {
			executed.Store(true)
			return nil
		},
	})
	require.NoError(t, start.Handle(context.Background()))
	select {
	case <-claimStarted:
	case <-time.After(time.Second):
		t.Fatal("restart claim update did not start")
	}
	close(releaseClaim)
	r.stopper.Stop()

	got := mustGetTestDaemonTask(t, store, 1, WithTaskIDCond(EQ, dt.ID))
	require.Len(t, got, 1)
	assert.Equal(t, task.TaskStatus_CancelRequested, got[0].TaskStatus)
	assert.Equal(t, "foreign-runner", got[0].TaskRunner)
	assert.Empty(t, got[0].Details.Error)
	assert.Equal(t, int64(1), updateCalls.Load())
	assert.False(t, executed.Load())
}
