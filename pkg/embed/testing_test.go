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
	"sync"
	"testing"
	"time"

	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type delayedTaskServiceGetter struct {
	mu      sync.RWMutex
	service taskservice.TaskService
	called  chan struct{}
	once    sync.Once
}

func newDelayedTaskServiceGetter() *delayedTaskServiceGetter {
	return &delayedTaskServiceGetter{called: make(chan struct{})}
}

func (g *delayedTaskServiceGetter) GetTaskService() (taskservice.TaskService, bool) {
	g.once.Do(func() { close(g.called) })
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.service, g.service != nil
}

func (g *delayedTaskServiceGetter) set(service taskservice.TaskService) {
	g.mu.Lock()
	g.service = service
	g.mu.Unlock()
}

func (g *delayedTaskServiceGetter) Start() error { return nil }
func (g *delayedTaskServiceGetter) Close() error { return nil }

func TestWaitTaskServiceReadyObservesOwnedService(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	service := taskservice.NewTaskService(
		mruntime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer func() { require.NoError(t, service.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- waitTaskServiceReady(ctx, getter, time.Millisecond)
	}()
	<-getter.called
	getter.set(service)
	require.NoError(t, <-done)
}

func TestWaitTaskServiceReadyHonorsCancellation(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- waitTaskServiceReady(ctx, getter, time.Hour)
	}()
	<-getter.called
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWaitBasicClusterTaskServicesRejectsMissingCN(t *testing.T) {
	err := waitBasicClusterTaskServices(context.Background(), &cluster{}, 1)
	require.ErrorContains(t, err, "service not found")
}

func TestWaitBasicClusterTaskServicesHonorsCNCount(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	service := taskservice.NewTaskService(
		mruntime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer func() { require.NoError(t, service.Close()) }()
	getter.set(service)

	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = getter
	c := &cluster{services: []*operator{cn}}

	err := waitBasicClusterTaskServices(context.Background(), c, 2)
	require.ErrorContains(t, err, "service not found")
}

func TestWaitBasicClusterTaskServicesRejectsUnsupportedService(t *testing.T) {
	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = &closeTrackingService{}
	c := &cluster{services: []*operator{cn}}

	err := waitBasicClusterTaskServices(context.Background(), c, 1)
	require.ErrorContains(t, err, "does not expose its task service")
}

func TestWaitBasicClusterTaskServicesReportsReadinessCancellation(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = getter
	c := &cluster{services: []*operator{cn}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitBasicClusterTaskServices(ctx, c, 1)
	require.ErrorContains(t, err, "task service did not become ready")
}

func TestBasicClusterUsesShortStartupRetryIntervals(t *testing.T) {
	services := []*operator{
		{serviceType: metadata.ServiceType_LOG, cfg: newServiceConfig()},
		{serviceType: metadata.ServiceType_TN, cfg: newServiceConfig()},
		{serviceType: metadata.ServiceType_CN, cfg: newServiceConfig()},
	}
	for _, service := range services {
		adjustBasicClusterService(service)
	}

	assert.Equal(t, time.Second, services[0].cfg.LogService.HAKeeperCheckInterval.Duration)
	assert.Equal(t, 500*time.Millisecond, services[0].cfg.LogService.HAKeeperBootstrapRetryInterval.Duration)
	assert.Equal(t, 100*time.Millisecond, services[1].cfg.HAKeeperRunningRetryInterval.Duration)
	assert.Equal(t, 100*time.Millisecond, services[2].cfg.TNShardReadyRetryInterval.Duration)
}

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
		// The package's shared base can already be active when this rollback
		// test runs. Bypass admission only to reach the injected partial-start
		// cleanup path; no second complete cluster is created.
		c.options.allowConcurrentTestClusters = true
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
