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

package cnservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/version"
)

type readinessCluster struct {
	clusterservice.MOCluster

	mu           sync.Mutex
	snapshots    [][]metadata.CNService
	current      []metadata.CNService
	refreshCalls int
	refreshHook  func(context.Context, int) error
}

type nonRefreshingCluster struct {
	clusterservice.MOCluster
}

func (c *readinessCluster) Refresh(ctx context.Context) error {
	c.mu.Lock()
	c.refreshCalls++
	call := c.refreshCalls
	hook := c.refreshHook
	c.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, call); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.snapshots) > 0 {
		idx := call - 1
		if idx >= len(c.snapshots) {
			idx = len(c.snapshots) - 1
		}
		c.current = append([]metadata.CNService(nil), c.snapshots[idx]...)
	}
	return nil
}

func (c *readinessCluster) GetCNServiceWithoutWorkingState(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.mu.Lock()
	services := append([]metadata.CNService(nil), c.current...)
	c.mu.Unlock()
	for _, cn := range services {
		if !apply(cn) {
			return
		}
	}
}

func (c *readinessCluster) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refreshCalls
}

func TestClusterSelfReadinessRequiresCurrentHeartbeatGeneration(t *testing.T) {
	const (
		serviceID = "cn-1"
		address   = "127.0.0.1:6002"
	)
	cluster := &readinessCluster{
		snapshots: [][]metadata.CNService{
			{{
				ServiceID:              serviceID,
				PipelineServiceAddress: "127.0.0.1:5002",
				CommitID:               version.CommitID,
			}},
			{{
				ServiceID:              serviceID,
				PipelineServiceAddress: address,
				CommitID:               version.CommitID + "-previous",
			}},
			{{
				ServiceID:              serviceID,
				PipelineServiceAddress: address,
				CommitID:               version.CommitID,
			}},
		},
	}
	heartbeatReady := make(chan struct{})
	close(heartbeatReady)
	s := &service{
		cfg: &Config{
			UUID:           serviceID,
			ServiceAddress: address,
		},
		logger:            zap.NewNop(),
		moCluster:         cluster,
		hakeeperConnected: heartbeatReady,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, s.waitForClusterSelfReadyWithContext(ctx, time.Millisecond))
	require.Equal(t, 3, cluster.calls())
}

func TestClusterSelfReadinessHonorsCancellationBeforeHeartbeat(t *testing.T) {
	cluster := &readinessCluster{}
	s := &service{
		cfg:               &Config{UUID: t.Name()},
		logger:            zap.NewNop(),
		moCluster:         cluster,
		hakeeperConnected: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.waitForClusterSelfReadyWithContext(ctx, time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "before startup deadline")
	require.Zero(t, cluster.calls())
}

func TestClusterSelfReadinessReportsAuthoritativeRefreshFailure(t *testing.T) {
	refreshErr := errors.New("hakeeper snapshot unavailable")
	ctx, cancel := context.WithCancel(context.Background())
	cluster := &readinessCluster{
		refreshHook: func(context.Context, int) error {
			cancel()
			return refreshErr
		},
	}
	heartbeatReady := make(chan struct{})
	close(heartbeatReady)
	s := &service{
		cfg:               &Config{UUID: t.Name()},
		logger:            zap.NewNop(),
		moCluster:         cluster,
		hakeeperConnected: heartbeatReady,
	}

	err := s.waitForClusterSelfReadyWithContext(ctx, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), refreshErr.Error())
	require.Equal(t, 1, cluster.calls())
}

func TestClusterSelfReadinessRejectsNonAuthoritativeCluster(t *testing.T) {
	heartbeatReady := make(chan struct{})
	close(heartbeatReady)
	s := &service{
		cfg:               &Config{UUID: t.Name()},
		logger:            zap.NewNop(),
		moCluster:         &nonRefreshingCluster{},
		hakeeperConnected: heartbeatReady,
	}

	err := s.waitForClusterSelfReadyWithContext(context.Background(), time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support authoritative refresh")
}

func TestServiceStartDoesNotBootstrapBeforeClusterSelfReady(t *testing.T) {
	moruntime.RunTest(t.Name(), func(moruntime.Runtime) {
		const address = "127.0.0.1:6002"
		refreshEntered := make(chan struct{})
		releaseRefresh := make(chan struct{})
		var enterOnce sync.Once
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
		t.Cleanup(release)
		cluster := &readinessCluster{
			snapshots: [][]metadata.CNService{{{
				ServiceID:              t.Name(),
				PipelineServiceAddress: address,
				CommitID:               version.CommitID,
			}}},
			refreshHook: func(ctx context.Context, _ int) error {
				enterOnce.Do(func() { close(refreshEntered) })
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-releaseRefresh:
					return nil
				}
			},
		}

		bootstrapErr := errors.New("stop after readiness gate")
		boot := &testBootService{bootstrapErr: bootstrapErr}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ls := mock_lock.NewMockLockService(ctrl)
		ls.EXPECT().Close().Return(nil).Times(2)
		cfg := &Config{
			UUID:           t.Name(),
			ServiceAddress: address,
		}
		cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
		cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
		cfg.Txn.Trace.BufferSize = 1
		heartbeatReady := make(chan struct{})
		close(heartbeatReady)
		s := &service{
			cfg:                cfg,
			logger:             zap.NewNop(),
			stopper:            stopper.NewStopper("test-cluster-readiness"),
			bootstrapService:   boot,
			mo:                 closeErrorMOServer{},
			cancelMoServerFunc: func() {},
			server:             closeOnlyRPCServer{},
			lockService:        ls,
			moCluster:          cluster,
			hakeeperConnected:  heartbeatReady,
		}
		s.options.traceDataPath = t.TempDir()

		startDone := make(chan error, 1)
		go func() {
			startDone <- s.Start()
		}()
		<-refreshEntered
		require.Zero(t, boot.bootstrapCount.Load())

		release()
		require.ErrorIs(t, <-startDone, bootstrapErr)
		require.Equal(t, int32(1), boot.bootstrapCount.Load())
	})
}
