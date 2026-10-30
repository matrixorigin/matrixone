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
	for _, tc := range []struct {
		name    string
		option  ISCPExecutorOption
		sync    time.Duration
		persist time.Duration
	}{
		{"defaults", ISCPExecutorOption{}, DefaultSyncTaskInterval, DefaultIndexFlushWatermarkInterval},
		{"configured", ISCPExecutorOption{SyncTaskInterval: 11 * time.Millisecond, IndexFlushWatermarkInterval: 23 * time.Millisecond}, 11 * time.Millisecond, 23 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.option
			var owner context.Context
			factory := ISCPTaskExecutorFactory(
				nil, nil,
				func(_ context.Context, _ uint64, routine taskservice.ActiveRoutine) error {
					exec := routine.(*ISCPTaskExecutor)
					owner = exec.ownerCtx
					require.Equal(t, tc.sync, exec.option.SyncTaskInterval)
					require.Equal(t, tc.persist, exec.option.IndexFlushWatermarkInterval)
					// A later attempt must receive a fresh option value.
					exec.option.SyncTaskInterval = time.Hour
					return attachErr
				}, "", nil, nil, tc.option,
			)
			for range 2 {
				require.ErrorIs(t, factory(context.Background(), &task.DaemonTask{}), attachErr)
				require.NotNil(t, owner)
				select {
				case <-owner.Done():
				default:
					t.Fatal("attach failure retained the executor lifetime")
				}
				require.Equal(t, original, tc.option)
			}
		})
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
