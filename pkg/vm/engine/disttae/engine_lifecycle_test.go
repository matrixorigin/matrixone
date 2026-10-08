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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestEngineGCSchedulerIsOwnedAndJoined(t *testing.T) {
	e := &Engine{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.StartGCScheduler(ctx))
	require.NoError(t, e.StartGCScheduler(ctx))
	require.NoError(t, e.Close())
	require.NoError(t, e.Close())
	require.Error(t, e.StartGCScheduler(context.Background()))
}

func TestEngineGCSchedulerRejectsCanceledContext(t *testing.T) {
	e := &Engine{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, e.StartGCScheduler(ctx), context.Canceled)
	require.NoError(t, e.Close())
}

func TestEngineCloseStopsGCSchedulerBeforePushClient(t *testing.T) {
	e := &Engine{}
	done := make(chan struct{})
	var pushClientClosed atomic.Bool
	e.gcSchedulerCancel = func() {
		pushClientClosed.Store(e.pClient.closed.Load())
		close(done)
	}
	e.gcSchedulerDone = done

	require.NoError(t, e.Close())
	require.False(t, pushClientClosed.Load())
	require.True(t, e.pClient.closed.Load())
}

// Observe the real constructor before any owner has been published. The option
// sentinel prevents unrelated initialization after test cleanup opens readiness.
func TestEngineConstructorReadinessCancellation(t *testing.T) {
	for _, pendingRefresh := range []bool{false, true} {
		name := "missing runtime cluster"
		if pendingRefresh {
			name = "builtin initial refresh pending"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sid := t.Name()
				rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger())
				runtime.SetupServiceBasedRuntime(sid, rt)
				rt.SetGlobalVariables(runtime.LockService, &prefetchTestLockService{id: sid})
				client := &pendingLogtailClusterClient{
					testHAKeeperClient: &testHAKeeperClient{}, started: make(chan struct{}),
				}
				var cluster clusterservice.MOCluster
				if pendingRefresh {
					cluster = clusterservice.NewMOCluster(sid, client, time.Hour)
					rt.SetGlobalVariables(runtime.ClusterService, cluster)
					<-client.started
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				finished := make(chan any, 1)
				published := make(chan struct{}, 1)
				const sentinel = "stop after readiness"
				go func() {
					defer func() { finished <- recover() }()
					New(ctx, sid, nil, nil, nil, nil, nil, 1,
						func(*Engine) { published <- struct{}{} },
						func(*Engine) { panic(sentinel) })
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				blocked := false
				var result any
				select {
				case result = <-finished:
				default:
					blocked = true
				}
				select {
				case <-published:
					t.Error("constructor owner was published before readiness")
				default:
				}
				// Always retire the real dependency and the constructor before
				// reporting the failed cancellation contract.
				if pendingRefresh {
					cluster.Close()
				} else {
					cluster = clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
					rt.SetGlobalVariables(runtime.ClusterService, cluster)
				}
				if blocked {
					result = <-finished
				}
				cluster.Close()
				if !blocked {
					err, ok := result.(error)
					if !ok || !errors.Is(err, context.Canceled) {
						t.Errorf("constructor returned unexpected outcome after cancellation: %v", result)
					}
				}
				if blocked {
					if result != sentinel {
						t.Errorf("test cleanup stopped at unexpected constructor phase: %v", result)
					}
					t.Errorf("canceled Engine.New remains blocked before owner publication; only retiring/publishing the cluster releases it")
				}
			})
		})
	}
}

func TestEngineConstructorRejectsEndedContext(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			expected := context.Canceled
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				expected = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			published := false
			var failure any
			func() {
				defer func() { failure = recover() }()
				New(ctx, t.Name(), nil, nil, nil, nil, nil, 1, func(*Engine) { published = true })
			}()
			require.ErrorIs(t, failure.(error), expected)
			require.False(t, published)
		})
	}
}

func TestEngineConstructorReadyFailureRetiresOwner(t *testing.T) {
	sid := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger())
	runtime.SetupServiceBasedRuntime(sid, rt)
	rt.SetGlobalVariables(runtime.LockService, &prefetchTestLockService{id: sid})
	cluster := clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
	defer cluster.Close()
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	var owner *Engine
	const failure = "failure after owner publication"
	require.PanicsWithValue(t, failure, func() {
		New(context.Background(), sid, nil, nil, nil, nil, nil, 1, func(e *Engine) { owner = e }, func(*Engine) { panic(failure) })
	})
	require.NotNil(t, owner)
	require.True(t, owner.gcPool.IsClosed(), "real acquired pool must retire before constructor panic escapes")
	require.NoError(t, owner.Close())
	require.Error(t, owner.StartGCScheduler(context.Background()))
}
