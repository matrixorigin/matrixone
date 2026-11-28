// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

type panicTestReporter struct{}

func (panicTestReporter) Helper() {}

func (panicTestReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestSharedTestClusterReportsInitializationError(t *testing.T) {
	state := SharedTestCluster{}
	wantErr := errors.New("cluster startup failed")
	initCalls := 0
	testCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return nil, wantErr
	}
	test := func(Cluster) {
		testCalls++
	}

	reporter := panicTestReporter{}
	for range 2 {
		require.PanicsWithValue(t,
			"failed to initialize shared cluster: cluster startup failed",
			func() {
				state.Run(reporter, init, test)
			},
		)
	}
	require.Equal(t, 1, initCalls)
	require.Zero(t, testCalls)
}

func TestSharedTestClusterRejectsNilInitialization(t *testing.T) {
	state := SharedTestCluster{}
	reporter := panicTestReporter{}

	require.PanicsWithValue(t,
		"failed to initialize shared cluster: internal error: cluster initializer returned nil without an error",
		func() {
			state.Run(reporter, func() (Cluster, error) {
				return nil, nil
			}, func(Cluster) {
				t.Fatal("test callback must not run")
			})
		},
	)
}

func TestSharedTestClusterClosesFailedInitialization(t *testing.T) {
	lease, err := acquireClusterPortLease()
	require.NoError(t, err)
	value := &cluster{
		portLease:     lease,
		portLeaseBase: lease.base,
		portLeaseNext: lease.base,
	}
	t.Cleanup(func() {
		if value.portLease != nil {
			require.NoError(t, value.releasePortLeaseLocked())
		}
	})

	state := SharedTestCluster{}
	require.PanicsWithValue(t,
		"failed to initialize shared cluster: cluster startup failed",
		func() {
			state.Run(panicTestReporter{}, func() (Cluster, error) {
				return value, errors.New("cluster startup failed")
			}, func(Cluster) {
				t.Fatal("test callback must not run")
			})
		},
	)
	require.Nil(t, value.portLease)
	require.Nil(t, state.cluster)
}

func TestSharedTestClusterRetainsFailedCleanupOwner(t *testing.T) {
	startErr := errors.New("cluster startup failed")
	closeErr := errors.New("cluster cleanup failed")
	service := &closeTrackingService{closeErr: closeErr}
	op := &operator{state: started}
	op.reset.svc = service
	value := &cluster{state: started, services: []*operator{op}}
	state := SharedTestCluster{}
	initCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return value, startErr
	}

	require.Panics(t, func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run")
		})
	})
	require.Same(t, value, state.cluster)
	require.ErrorIs(t, state.err, startErr)
	require.ErrorIs(t, state.err, closeErr)
	require.Equal(t, int32(1), service.closeCount.Load())

	service.closeErr = nil
	require.Panics(t, func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run")
		})
	})
	require.Equal(t, 1, initCalls)
	require.Equal(t, int32(2), service.closeCount.Load())
	require.Nil(t, state.cluster)
	require.NoError(t, state.Close())
}

func TestStartTestClusterReturnsCleanupOwnerOnRollbackFailure(t *testing.T) {
	startErr := errors.New("cluster startup failed")
	closeErr := errors.New("cluster cleanup failed")
	service := &closeTrackingService{closeErr: closeErr}

	value, err := StartTestCluster(Option(func(c *cluster) {
		c.startFn = func(op *operator) error {
			op.state = started
			op.reset.svc = service
			return startErr
		}
	}))
	if value != nil {
		cleanupOwner := value
		t.Cleanup(func() {
			service.closeErr = nil
			require.NoError(t, cleanupOwner.Close())
		})
	}

	require.ErrorIs(t, err, startErr)
	require.ErrorIs(t, err, closeErr)
	require.NotNil(t, value)
	require.Equal(t, int32(2), service.closeCount.Load())

	service.closeErr = nil
	require.NoError(t, value.Close())
	require.Equal(t, int32(3), service.closeCount.Load())
}

func TestSharedTestClusterCloseIsTerminal(t *testing.T) {
	state := SharedTestCluster{}
	value := &cluster{}
	initCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return value, nil
	}

	state.Run(panicTestReporter{}, init, func(cluster Cluster) {
		require.Same(t, value, cluster)
	})
	require.NoError(t, state.Close())
	require.True(t, state.closed)
	require.Nil(t, state.cluster)

	require.PanicsWithValue(t, "shared cluster is closed", func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run after close")
		})
	})
	require.Equal(t, 1, initCalls)
	require.NoError(t, state.Close())
}

type basicFixtureTaskService struct{ taskservice.TaskService }

func TestBasicTaskServicePublication(t *testing.T) {
	rt := mruntime.DefaultRuntime()
	one, two := &basicFixtureTaskService{}, &basicFixtureTaskService{}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	require.False(t, basicTaskServicePublished(rt))
	rt.SetGlobalVariables("task-service", one)
	require.False(t, basicTaskServicePublished(rt))
	rt.SetGlobalVariables("parameter-unit", pu)
	require.False(t, basicTaskServicePublished(rt))
	pu.SetTaskService(two)
	require.False(t, basicTaskServicePublished(rt))
	pu.SetTaskService(one)
	require.True(t, basicTaskServicePublished(rt))
	rt.SetGlobalVariables("task-service", two)
	require.False(t, basicTaskServicePublished(rt), "a new service generation is not published until the PU matches")
	pu.SetTaskService(two)
	require.True(t, basicTaskServicePublished(rt))
}

func TestWaitForBasicTaskService(t *testing.T) {
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		called := false
		err := waitForBasicTaskService(ctx, func() bool { called = true; return true })
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, called)
	})
	t.Run("expired", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		require.ErrorIs(t, waitForBasicTaskService(ctx, func() bool { return true }), context.DeadlineExceeded)
	})
	t.Run("cancel during publication", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		err := waitForBasicTaskService(ctx, func() bool { cancel(); return true })
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("cancel during later publication", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		err := waitForBasicTaskService(ctx, func() bool {
			calls++
			if calls == 1 {
				return false
			}
			cancel()
			return true
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, calls)
	})
	t.Run("cancel while waiting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		observed := make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			done <- waitForBasicTaskService(ctx, func() bool {
				select {
				case observed <- struct{}{}:
				default:
				}
				return false
			})
		}()
		<-observed
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
	t.Run("later publication", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var ready atomic.Bool
		observed := make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			done <- waitForBasicTaskService(ctx, func() bool {
				select {
				case observed <- struct{}{}:
				default:
				}
				return ready.Load()
			})
		}()
		<-observed
		ready.Store(true)
		require.NoError(t, <-done)
	})
}

type readinessCleanupCluster struct {
	Cluster
	calls    int
	closeErr error
}

func (c *readinessCleanupCluster) Close() error {
	c.calls++
	return c.closeErr
}

func TestBasicClusterReadinessFailureRetainsCleanupOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rt := mruntime.DefaultRuntime()
	t.Run("close succeeds", func(t *testing.T) {
		c := &readinessCleanupCluster{}
		value, err := finishBasicClusterInitialization(ctx, c, rt)
		require.Nil(t, value)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, c.calls)
	})
	t.Run("close fails", func(t *testing.T) {
		closeErr := errors.New("close failed")
		c := &readinessCleanupCluster{closeErr: closeErr}
		value, err := finishBasicClusterInitialization(ctx, c, rt)
		require.Same(t, c, value)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, closeErr)
		require.Equal(t, 1, c.calls)
		c.closeErr = nil
		require.NoError(t, value.Close())
		require.Equal(t, 2, c.calls)
	})
}

func TestStartBasicClusterCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c, err := startBasicCluster(ctx)
	require.Nil(t, c)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBasicClusterReusesOwnedTaskService(t *testing.T) {
	var firstPU *config.ParameterUnit
	var firstService taskservice.TaskService
	for i := 0; i < 3; i++ {
		RunBaseClusterTests(t, func(c Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			owner, ok := cn.RawService().(cnservice.Service)
			require.True(t, ok)
			service, ready := owner.GetTaskService()
			require.True(t, ready)
			require.NotNil(t, service)
			rt := mruntime.ServiceRuntime(cn.ServiceID())
			value, ok := rt.GetGlobalVariables("task-service")
			require.True(t, ok)
			require.Same(t, service, value)
			value, ok = rt.GetGlobalVariables("parameter-unit")
			require.True(t, ok)
			pu, ok := value.(*config.ParameterUnit)
			require.True(t, ok)
			require.Same(t, service, pu.GetTaskService())
			if i == 0 {
				firstPU, firstService = pu, service
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					_, err := service.QueryDaemonTask(ctx)
					if err == nil {
						break
					}
					select {
					case <-ctx.Done():
						require.NoError(t, err, "CN-owned task service must execute a real query")
						return
					case <-ticker.C:
					}
				}
			} else {
				require.Same(t, firstPU, pu)
				require.Same(t, firstService, service)
			}
		})
	}
}
