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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
	require.NoError(t, s.waitForClusterSelfReadyWithContext(ctx, time.Millisecond, false))
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

	err := s.waitForClusterSelfReadyWithContext(ctx, time.Millisecond, false)
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

	err := s.waitForClusterSelfReadyWithContext(ctx, time.Second, false)
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

	err := s.waitForClusterSelfReadyWithContext(context.Background(), time.Second, false)
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

// Use the real snapshot policy; a mock inventory alone cannot distinguish raw
// registration from an admission-aware query candidate.
type readinessClusterClient func(context.Context) (logpb.ClusterDetails, error)

func (f readinessClusterClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	return f(ctx)
}

func TestClusterQueryReadinessSnapshot(t *testing.T) {
	moruntime.RunTest(t.Name(), func(moruntime.Runtime) {
		var details atomic.Pointer[logpb.ClusterDetails]
		details.Store(&logpb.ClusterDetails{})
		cluster := clusterservice.NewMOCluster(t.Name(), readinessClusterClient(func(context.Context) (logpb.ClusterDetails, error) { return *details.Load(), nil }), time.Hour)
		t.Cleanup(cluster.Close)
		s := &service{cfg: &Config{UUID: t.Name(), ServiceAddress: "127.0.0.1:6002"}, moCluster: cluster, viewMetadataAdmissionGeneration: 11}
		self := logpb.CNStore{UUID: t.Name(), ServiceAddress: s.pipelineServiceServiceAddr(), CommitID: version.CommitID, ViewMetadataAdmissionGeneration: 11, ViewMetadataAdmissionReady: true}
		for _, test := range []struct {
			name                    string
			mutate                  func(*logpb.CNStore)
			active, preparing, want bool
		}{
			{name: "ready", active: true, want: true},
			{name: "pending", active: true, mutate: func(c *logpb.CNStore) { c.ViewMetadataAdmissionReady = false }},
			{name: "preparing ready", preparing: true, want: true},
			{name: "preparing pending", preparing: true, mutate: func(c *logpb.CNStore) { c.ViewMetadataAdmissionReady = false }},
			{name: "disabled admission", want: true, mutate: func(c *logpb.CNStore) { c.ViewMetadataAdmissionReady = false }},
			{name: "other CN", active: true, mutate: func(c *logpb.CNStore) { c.UUID = "other" }},
			{name: "old address", active: true, mutate: func(c *logpb.CNStore) { c.ServiceAddress = "old:6002" }},
			{name: "old commit", active: true, mutate: func(c *logpb.CNStore) { c.CommitID = "old" }},
			{name: "old generation", active: true, mutate: func(c *logpb.CNStore) { c.ViewMetadataAdmissionGeneration = 10 }},
			{name: "disabled old generation", mutate: func(c *logpb.CNStore) { c.ViewMetadataAdmissionGeneration = 10 }},
		} {
			t.Run(test.name, func(t *testing.T) {
				row := self
				if test.mutate != nil {
					test.mutate(&row)
				}
				details.Store(&logpb.ClusterDetails{CNStores: []logpb.CNStore{row}, ViewMetadataAdmission: &logpb.ViewMetadataAdmission{Enabled: test.active, Preparing: test.preparing}})
				require.NoError(t, cluster.(clusterservice.AuthoritativeRefresher).Refresh(t.Context()))
				ready, err := s.clusterSnapshotContainsSelf(t.Context(), true)
				require.NoError(t, err)
				require.Equal(t, test.want, ready)
				if test.name == "pending" {
					ready, err = s.clusterSnapshotContainsSelf(t.Context(), false)
					require.NoError(t, err)
					require.True(t, ready)
				}
			})
		}
	})
}

func TestClusterQueryReadinessFailures(t *testing.T) {
	for _, failure := range []string{"owner error", "owner cancelled", "revoked", "refresh error"} {
		t.Run(failure, func(t *testing.T) {
			heartbeatReady := make(chan struct{})
			close(heartbeatReady)
			ownerCtx, cancelOwner := context.WithCancel(context.Background())
			defer cancelOwner()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			expected := errors.New(failure)
			cluster := &readinessCluster{current: []metadata.CNService{{ServiceID: t.Name(), PipelineServiceAddress: "127.0.0.1:6002", CommitID: version.CommitID, ViewMetadataAdmissionGeneration: 11}}}
			s := &service{cfg: &Config{UUID: t.Name(), ServiceAddress: "127.0.0.1:6002"}, logger: zap.NewNop(), moCluster: cluster, hakeeperConnected: heartbeatReady, viewMetadataAdmissionGeneration: 11, bootstrapUpgradeContext: ownerCtx, bootstrapUpgradeResult: result}
			cluster.refreshHook = func(context.Context, int) error {
				switch failure {
				case "owner error":
					result <- expected
				case "owner cancelled":
					cancelOwner()
				case "revoked":
					s.viewMetadataGenerationRevoked.Store(true)
				case "refresh error":
					cancel()
					return expected
				}
				return nil
			}
			err := s.waitForClusterSelfReadyWithContext(ctx, time.Hour, true)
			require.Error(t, err)
			switch failure {
			case "owner error":
				require.ErrorIs(t, err, expected)
			case "owner cancelled":
				require.ErrorIs(t, err, context.Canceled)
			case "revoked":
				require.Contains(t, err.Error(), "revoked")
			case "refresh error":
				require.Contains(t, err.Error(), expected.Error())
			}
			require.Equal(t, 1, cluster.calls())
		})
	}
}

type readinessFrontend struct {
	closeErrorMOServer
	starts atomic.Int32
	stops  atomic.Int32
}

func (f *readinessFrontend) Start() error {
	f.starts.Add(1)
	return nil
}

func (f *readinessFrontend) Stop() error {
	f.stops.Add(1)
	return nil
}

type readinessPipeline struct {
	closeOnlyRPCServer
	starts atomic.Int32
	closes atomic.Int32
}

func (p *readinessPipeline) Start() error {
	p.starts.Add(1)
	return nil
}

func (p *readinessPipeline) Close() error {
	p.closes.Add(1)
	return nil
}

// This proves the Start call ordering and rollback ownership with real
// clusterservice admission policy, independent of the predicate unit tests.
func TestServiceStartRequiresQuerySnapshot(t *testing.T) {
	for _, mode := range []string{"success", "deadline", "owner error"} {
		t.Run(mode, func(t *testing.T) {
			refreshEntered := make(chan struct{})
			releaseRefresh := make(chan struct{})
			failOwner := make(chan struct{})
			var refreshOnce, releaseOnce, failOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
			fail := func() { failOnce.Do(func() { close(failOwner) }) }
			t.Cleanup(release)
			t.Cleanup(fail)
			ownerFailure := errors.New("startup owner failed during candidate convergence")
			refreshFailure := errors.New("startup authoritative refresh unavailable")
			boot := &testBootService{}
			if mode == "owner error" {
				boot.bootstrapUpgradeHook = func(ctx context.Context) error {
					select {
					case <-failOwner:
						return ownerFailure
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if sql == catalog.ViewMetadataLifecycleGateSQL {
					return viewMetadataLifecycleGateTestResult(), nil
				}
				return executor.Result{}, nil
			})
			s := newViewMetadataAdmissionStartService(t, boot, exec, 5*time.Second)
			s.cfg.ServiceAddress = "127.0.0.1:6002"
			s.cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
			if mode == "deadline" {
				s.cfg.HAKeeper.DiscoveryTimeout.Duration = 250 * time.Millisecond
			}
			frontend := &readinessFrontend{}
			pipeline := &readinessPipeline{}
			s.mo = frontend
			s.server = pipeline
			s.heartbeatWakeup = make(chan struct{}, 1)
			s.hakeeperConnected = make(chan struct{})
			close(s.hakeeperConnected)
			self := logpb.CNStore{UUID: s.cfg.UUID, ServiceAddress: s.pipelineServiceServiceAddr(), CommitID: version.CommitID, ViewMetadataAdmissionGeneration: 11}
			client := readinessClusterClient(func(ctx context.Context) (logpb.ClusterDetails, error) {
				row := self
				if s.viewMetadataIngressReady.Load() {
					refreshOnce.Do(func() { close(refreshEntered) })
					select {
					case <-releaseRefresh:
						row.ViewMetadataAdmissionReady = true
					case <-ctx.Done():
						if mode == "deadline" {
							return logpb.ClusterDetails{}, refreshFailure
						}
						return logpb.ClusterDetails{}, ctx.Err()
					}
				}
				return logpb.ClusterDetails{CNStores: []logpb.CNStore{row}, ViewMetadataAdmission: &logpb.ViewMetadataAdmission{Enabled: true, Epoch: 5}}, nil
			})
			cluster := clusterservice.NewMOCluster(s.cfg.UUID, client, time.Hour)
			t.Cleanup(cluster.Close)
			s.moCluster = cluster
			// Drain only the initial background refresh. Later refreshes belong to
			// Start's bounded context; a static snapshot option would disable them.
			initialCtx, cancelInitial := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelInitial()
			require.NoError(t, clusterservice.GetCNServiceRawWithContext(initialCtx, cluster, clusterservice.NewSelector(), func(metadata.CNService) bool { return true }))
			t.Cleanup(func() {
				release()
				fail()
				require.NoError(t, s.Close())
			})
			startDone := make(chan error, 1)
			go func() { startDone <- s.Start() }()
			select {
			case <-refreshEntered:
			case err := <-startDone:
				t.Fatalf("Start returned before authoritative query-candidate refresh: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not reach final query-candidate refresh")
			}
			require.Equal(t, int32(1), pipeline.starts.Load())
			require.True(t, s.viewMetadataIngressReady.Load())
			require.Zero(t, frontend.starts.Load(), "SQL acceptance opened before candidate convergence")
			require.False(t, s.task.runnerReady.Load())
			select {
			case <-s.heartbeatWakeup:
			default:
				t.Fatal("ingress publication did not wake the existing heartbeat owner")
			}
			if mode == "success" {
				release()
			} else if mode == "owner error" {
				fail()
			}
			var err error
			select {
			case err = <-startDone:
			case <-time.After(5 * time.Second):
				t.Fatal("candidate wait or rollback did not terminate")
			}
			if mode == "success" {
				require.NoError(t, err)
				require.Equal(t, int32(1), frontend.starts.Load())
				require.Equal(t, serviceStarted, s.lifecycle)
			} else {
				require.Error(t, err)
				if mode == "owner error" {
					require.ErrorIs(t, err, ownerFailure)
				} else {
					require.Contains(t, err.Error(), refreshFailure.Error())
				}
				require.Zero(t, frontend.starts.Load())
				require.False(t, s.task.runnerReady.Load())
				require.Equal(t, serviceClosed, s.lifecycle)
				require.Equal(t, int32(1), frontend.stops.Load())
				require.Equal(t, int32(1), pipeline.closes.Load())
			}
		})
	}
}
