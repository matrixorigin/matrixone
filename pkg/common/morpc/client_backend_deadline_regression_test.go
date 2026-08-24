// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type lateSuccessfulBackendFactory struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func (f *lateSuccessfulBackendFactory) Create(
	string,
	...BackendOption,
) (Backend, error) {
	f.calls.Add(1)
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return &testBackend{activeTime: time.Now()}, nil
}

// TestAutoCreateDeadlineRejectsLateSuccessfulBackend reproduces the nightly
// regression shape: many requests coalesce on one backend creation, the
// factory succeeds after a 500ms wait budget, and every coalesced request
// observes ErrBackendClosed (20502) even though the backend is published and
// usable immediately afterwards.
func TestAutoCreateDeadlineRejectsLateSuccessfulBackend(t *testing.T) {
	const callers = 100
	factory := &lateSuccessfulBackendFactory{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	core, observedLogs := observer.New(zap.WarnLevel)
	rpcClient, err := NewClient(
		t.Name(),
		factory,
		WithClientEnableAutoCreateBackend(),
		WithClientAutoCreateWaitTimeout(500*time.Millisecond),
		WithClientDisableCircuitBreaker(),
		WithClientLogger(zap.New(core)),
	)
	require.NoError(t, err)
	client := rpcClient.(*client)
	timeoutsBefore := testutil.ToFloat64(client.metrics.autoCreateTimeoutCounter)
	eventsBefore := testutil.ToFloat64(client.metrics.autoCreateTimeoutEventCounter)

	var releaseOnce sync.Once
	releaseFactory := func() {
		releaseOnce.Do(func() { close(factory.release) })
	}
	t.Cleanup(func() {
		releaseFactory()
		require.NoError(t, rpcClient.Close())
	})

	type result struct {
		future *Future
		err    error
	}
	results := make(chan result, callers)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := range callers {
		go func(id int) {
			ready.Done()
			<-start
			future, sendErr := rpcClient.Send(
				context.Background(),
				"remote-lock-service",
				newTestMessage(uint64(id+1)),
			)
			results <- result{
				future: future,
				err:    sendErr,
			}
		}(i)
	}
	ready.Wait()
	close(start)

	select {
	case <-factory.entered:
	case <-time.After(time.Second):
		t.Fatal("backend factory did not start")
	}

	for range callers {
		select {
		case got := <-results:
			if got.future != nil {
				got.future.Close()
			}
			require.ErrorIs(t, got.err, ErrBackendCreateTimeout)
			require.True(t,
				moerr.IsMoErrCode(got.err, moerr.ErrBackendClosed),
				"late successful create surfaced as %v", got.err)
		case <-time.After(2 * time.Second):
			t.Fatal("coalesced request did not hit the backend-create deadline")
		}
	}

	require.EqualValues(t, 1, factory.calls.Load())
	require.Equal(t, float64(callers),
		testutil.ToFloat64(client.metrics.autoCreateTimeoutCounter)-timeoutsBefore)
	require.Equal(t, float64(1),
		testutil.ToFloat64(client.metrics.autoCreateTimeoutEventCounter)-eventsBefore)
	require.Len(t, observedLogs.All(), 1,
		"coalesced waiters must produce one rate-limited lifecycle warning")
	require.Equal(t, 1, client.autoCreateTimeoutLogger.StateCount())
	logFields := observedLogs.All()[0].ContextMap()
	require.Equal(t, "backend-auto-create-timeout", logFields["event"])
	require.Equal(t, t.Name(), logFields["client"])
	require.Equal(t, "create-state", logFields["scope"])
	releaseFactory()
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.mu.backends["remote-lock-service"]) == 1
	}, time.Second, time.Millisecond)

	postCreateCtx, cancelPostCreate := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancelPostCreate()
	future, err := rpcClient.Send(
		postCreateCtx,
		"remote-lock-service",
		newTestMessage(callers+1),
	)
	require.NoError(t, err)
	require.NotNil(t, future)
	defer future.Close()
	_, err = future.Get()
	require.NoError(t, err)
}
