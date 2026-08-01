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
