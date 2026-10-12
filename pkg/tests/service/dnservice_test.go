// Copyright 2024 Matrix Origin
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

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
)

func Test_buildTNOptions(t *testing.T) {
	//opts := buildTNOptions(&tnservice.Config{}, nil)
	//for i, opt := range opts {
	//
	//}

	moruntime.SetupServiceBasedRuntime("", moruntime.DefaultRuntime())
	defer leaktest.AfterTest(t)()
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}
	ctx := context.Background()

	// initialize cluster
	c, err := NewCluster(ctx, t, DefaultOptions().
		WithCNServiceNum(1).
		WithTNServiceNum(1).
		WithTNShardNum(1))
	require.NoError(t, err)

	// close the cluster
	defer func(c Cluster) {
		require.NoError(t, c.Close())
	}(c)
	// start the cluster
	require.NoError(t, c.Start())
}

type lifecycleTN struct {
	tnservice.Service
	startErr error
	closeErr error
	closes   int
	starts   int
}

func (s *lifecycleTN) Start() error { s.starts++; return s.startErr }
func (s *lifecycleTN) Close() error { s.closes++; return s.closeErr }

func TestTNWrapperClosesAcquiredBackend(t *testing.T) {
	failure := errors.New("TN close incomplete")
	for _, tc := range []struct {
		name               string
		status             ServiceStatus
		startErr, closeErr error
	}{
		{name: "initialized", status: ServiceInitialized},
		{name: "failed Start", status: ServiceInitialized, startErr: errors.New("start refused")},
		{name: "incomplete Close", status: ServiceStarted, closeErr: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &lifecycleTN{startErr: tc.startErr, closeErr: tc.closeErr}
			owner := &tnService{status: tc.status, svc: backend}
			t.Cleanup(func() { _ = owner.Close() })
			if tc.startErr != nil {
				require.Same(t, tc.startErr, owner.Start())
			}
			for i := 0; i < 2; i++ {
				require.Equal(t, tc.closeErr, owner.Close())
			}
			require.Equal(t, 1, backend.closes)
			require.Error(t, owner.Start())
			task, ok := owner.GetTaskService()
			require.Nil(t, task)
			require.False(t, ok)
			if tc.closeErr == nil {
				require.Equal(t, ServiceClosed, owner.Status())
			} else {
				require.Equal(t, ServiceStarted, owner.Status())
			}
		})
	}
}

func TestLogWrapperCloseBeforeStartIsTerminal(t *testing.T) {
	owner := &logService{status: ServiceInitialized}
	require.NoError(t, owner.Close())
	require.Equal(t, ServiceClosed, owner.Status())
	require.NoError(t, owner.Close())
}
