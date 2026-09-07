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

package process

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcquireSequenceSharesGateWithChildProcesses(t *testing.T) {
	base := &BaseProcess{}
	parent := &Process{Base: base}
	child := &Process{Base: base}

	release, err := parent.AcquireSequence(context.Background())
	require.NoError(t, err)
	require.Same(t, parent.Base, child.Base)
	// The permit is held before the child starts, so a correctly shared gate
	// has no available token regardless of goroutine scheduling.
	select {
	case <-base.sequenceGate.sem:
		t.Fatal("sequence gate unexpectedly has an available permit")
	default:
	}

	entered := make(chan struct{})
	finished := make(chan struct{})
	var childRelease func()
	var childErr error
	go func() {
		close(entered)
		childRelease, childErr = child.AcquireSequence(context.Background())
		close(finished)
	}()
	<-entered
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-finished:
		t.Fatal("child acquired sequence gate while parent held it")
	case <-timer.C:
	}

	release()
	<-finished
	require.NoError(t, childErr)
	childRelease()
	// Release is idempotent and must not overfill the semaphore.
	childRelease()
	followup, err := parent.AcquireSequence(context.Background())
	require.NoError(t, err)
	followup()
}

func TestAcquireSequenceCancellationDoesNotLeakPermit(t *testing.T) {
	proc := &Process{Base: &BaseProcess{}}
	release, err := proc.AcquireSequence(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	waiterStarted := make(chan struct{})
	acquireErrCh := make(chan error, 1)
	go func() {
		close(waiterStarted)
		permit, acquireErr := proc.AcquireSequence(ctx)
		if acquireErr == nil {
			permit()
		}
		acquireErrCh <- acquireErr
	}()
	<-waiterStarted
	// The holder keeps the shared semaphore empty while the waiter is queued.
	select {
	case <-proc.Base.sequenceGate.sem:
		t.Fatal("sequence gate unexpectedly has an available permit")
	default:
	}
	cancel()
	require.ErrorIs(t, <-acquireErrCh, context.Canceled)
	release()

	done := make(chan struct{})
	go func() {
		permit, acquireErr := proc.AcquireSequence(context.Background())
		if acquireErr == nil {
			permit()
		}
		acquireErrCh <- acquireErr
		close(done)
	}()
	<-done
	require.NoError(t, <-acquireErrCh)
}
