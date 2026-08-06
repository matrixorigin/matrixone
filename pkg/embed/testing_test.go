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
	"errors"
	"fmt"
	"testing"

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
