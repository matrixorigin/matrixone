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

package iscp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

type blockedRecoveryClient struct {
	client.TxnClient
	entered   chan struct{}
	resultErr error
}

func (c *blockedRecoveryClient) New(ctx context.Context, _ timestamp.Timestamp, _ ...client.TxnOption) (client.TxnOperator, error) {
	close(c.entered)
	<-ctx.Done()
	if c.resultErr != nil {
		return nil, c.resultErr
	}
	return nil, ctx.Err()
}

type stoppedRecoveryWorker struct{}

func (*stoppedRecoveryWorker) Submit(*IterationContext) error { return nil }
func (*stoppedRecoveryWorker) Stop()                          {}

func TestRecoveryRemainsFenceableAndPauseInterruptsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	owner, terminate := context.WithCancel(ctx)
	defer terminate()
	w := &stoppedRecoveryWorker{}
	blocked := &blockedRecoveryClient{entered: make(chan struct{})}
	exec := &ISCPTaskExecutor{cnUUID: "recovery-test", ownerCtx: owner, terminate: terminate, running: true, worker: w, cancel: func() {}, txnEngine: statusTestEngine{}, cnTxnClient: blocked, option: &ISCPExecutorOption{RetryTimes: 1}}
	RegisterExecutorRuntime(exec.cnUUID, exec)
	t.Cleanup(func() { UnregisterExecutorRuntime(exec.cnUUID, exec) })
	recovered := make(chan struct{})
	go func() { exec.abandonWorker(w); close(recovered) }()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("recovery did not begin")
	}
	got, ok := GetExecutorRuntime(exec.cnUUID)
	require.True(t, ok, "DDL must be able to fence while recovery waits on its transaction")
	require.Same(t, exec, got)
	for _, key := range []JobRuntimeKey{NewJobRuntimeKey(1, 2, "first", 3), NewJobRuntimeKey(1, 2, "second", 4)} {
		require.NoError(t, got.CancelAndDrainJobConsumer(ctx, key.AccountID, key.TableID, key.JobName, key.JobID))
		t.Cleanup(func() { exec.RemoveJobFence(key) })
	}
	paused := make(chan struct{})
	go func() { _ = exec.Pause(); close(paused) }()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("pause did not interrupt recovery")
	}
	select {
	case <-recovered:
	case <-ctx.Done():
		t.Fatal("recovery did not stop")
	}
	require.NoError(t, owner.Err(), "pause must not terminate the daemon lifetime")
	_, ok = GetExecutorRuntime(exec.cnUUID)
	require.False(t, ok)
}

func TestPauseInterruptsInitialStateLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	owner, terminate := context.WithCancel(ctx)
	defer terminate()
	blocked := &blockedRecoveryClient{entered: make(chan struct{})}
	exec := &ISCPTaskExecutor{ownerCtx: owner, terminate: terminate, txnEngine: statusTestEngine{}, cnTxnClient: blocked, option: &ISCPExecutorOption{RetryTimes: 1}}
	started := make(chan error, 1)
	go func() { started <- exec.Start() }()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("initial state load did not begin")
	}
	paused := make(chan struct{})
	go func() { _ = exec.Pause(); close(paused) }()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("pause did not interrupt initial state load")
	}
	require.ErrorIs(t, <-started, context.Canceled)
	require.NoError(t, owner.Err())
}

func TestResumeAfterPauseStartsAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	owner, terminate := context.WithCancel(ctx)
	defer terminate()
	blocked := &blockedRecoveryClient{entered: make(chan struct{})}
	exec := &ISCPTaskExecutor{ownerCtx: owner, terminate: terminate, txnEngine: statusTestEngine{}, cnTxnClient: blocked, option: &ISCPExecutorOption{RetryTimes: 1}}
	require.NoError(t, exec.Pause())
	resumed := make(chan error, 1)
	go func() { resumed <- exec.Resume() }()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("resume did not start state loading")
	}
	require.NoError(t, exec.Pause())
	require.ErrorIs(t, <-resumed, context.Canceled)
	require.NoError(t, owner.Err())
}

func TestPauseDoesNotReplaceStartupError(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	startupErr := errors.New("state load failed")
	blocked := &blockedRecoveryClient{entered: make(chan struct{}), resultErr: startupErr}
	exec := &ISCPTaskExecutor{ownerCtx: ctx, txnEngine: statusTestEngine{}, cnTxnClient: blocked, option: &ISCPExecutorOption{RetryTimes: 5}}
	started := make(chan error, 1)
	go func() { started <- exec.start(false) }()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("initial state load did not begin")
	}
	require.NoError(t, exec.Pause())
	err := <-started
	require.ErrorIs(t, err, startupErr)
	require.ErrorIs(t, err, errExecutorPaused)
}

func TestFactoryRetainsPausedExecutor(t *testing.T) {
	for _, tc := range []struct {
		name          string
		pauseAtAttach bool
		backendError  bool
	}{
		{name: "before initial start", pauseAtAttach: true},
		{name: "during initial load"},
		{name: "backend error during pause", backendError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			running.Store(false)
			t.Cleanup(func() { running.Store(false) })
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			blocked := &blockedRecoveryClient{entered: make(chan struct{})}
			if tc.backendError {
				blocked.resultErr = errors.New("backend failure")
			}
			attached := make(chan *ISCPTaskExecutor, 1)
			factory := ISCPTaskExecutorFactory(statusTestEngine{}, blocked, func(_ context.Context, _ uint64, routine taskservice.ActiveRoutine) error {
				exec := routine.(*ISCPTaskExecutor)
				if tc.pauseAtAttach {
					if err := exec.Pause(); err != nil {
						return err
					}
					if err := exec.start(false); !errors.Is(err, errExecutorPaused) {
						return errors.New("initial start ignored pause")
					}
				}
				attached <- exec
				return nil
			}, "", nil, nil)
			done := make(chan error, 1)
			go func() { done <- factory(ctx, &task.DaemonTask{}) }()
			var exec *ISCPTaskExecutor
			select {
			case exec = <-attached:
			case <-ctx.Done():
				t.Fatal("executor did not attach")
			}
			if !tc.pauseAtAttach {
				select {
				case <-blocked.entered:
				case <-ctx.Done():
					t.Fatal("initial load did not start")
				}
				require.NoError(t, exec.Pause())
			}
			require.NoError(t, exec.ownerCtx.Err())
			select {
			case err := <-done:
				t.Fatalf("factory exited while paused: %v", err)
			default:
			}
			require.NoError(t, exec.Cancel())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("factory did not exit after cancel")
			}
		})
	}
}

func TestExecutorFactoryReturnsAttachErrorAndReleasesAdmission(t *testing.T) {
	running.Store(false)
	t.Cleanup(func() { running.Store(false) })

	attachErr := errors.New("attach failed")
	factory := ISCPTaskExecutorFactory(
		nil,
		nil,
		func(context.Context, uint64, taskservice.ActiveRoutine) error {
			return attachErr
		},
		"",
		nil,
		nil,
	)

	for range 2 {
		require.ErrorIs(t, factory(context.Background(), &task.DaemonTask{}), attachErr)
	}
}

func TestExecutorLifetimeSurvivesPauseAndEndsOnCancel(t *testing.T) {
	ownerCtx, terminate := context.WithCancel(context.Background())
	exec := &ISCPTaskExecutor{ownerCtx: ownerCtx, terminate: terminate}
	returned := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		exec.waitForTermination(context.Background())
		close(returned)
	}()
	<-started

	require.NoError(t, exec.Pause())
	select {
	case <-returned:
		t.Fatal("pause terminated the daemon executor lifetime")
	default:
	}

	require.NoError(t, exec.Cancel())
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("cancel did not terminate the daemon executor lifetime")
	}
	require.Error(t, exec.Start())
}

func TestExecutorLifetimeEndsWithOwnerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	exec := &ISCPTaskExecutor{ownerCtx: ctx}
	returned := make(chan struct{})
	go func() {
		exec.waitForTermination(ctx)
		close(returned)
	}()
	cancel()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("owner cancellation did not terminate the daemon executor lifetime")
	}
}

func TestCancelSignalsLifetimeBeforeWaitingForStateLock(t *testing.T) {
	ownerCtx, terminate := context.WithCancel(context.Background())
	exec := &ISCPTaskExecutor{ownerCtx: ownerCtx, terminate: terminate}
	exec.runningMu.Lock()
	locked := true
	defer func() {
		if locked {
			exec.runningMu.Unlock()
		}
	}()

	returned := make(chan struct{})
	go func() {
		_ = exec.Cancel()
		close(returned)
	}()

	select {
	case <-ownerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("cancel waited for the state lock before signaling termination")
	}
	exec.runningMu.Unlock()
	locked = false
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("cancel did not finish after the state lock was released")
	}
}
