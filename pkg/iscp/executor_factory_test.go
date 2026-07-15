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
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

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
