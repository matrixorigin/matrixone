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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type admissionRevocationMOServer struct {
	stopped      chan struct{}
	startEntered chan struct{}
	releaseStart chan struct{}
	startOnce    sync.Once
	stopOnce     sync.Once
	startCount   atomic.Int32
}

func (s *admissionRevocationMOServer) GetRoutineManager() *frontend.RoutineManager {
	return nil
}

func (s *admissionRevocationMOServer) Start() error {
	s.startCount.Add(1)
	if s.startEntered != nil {
		s.startOnce.Do(func() { close(s.startEntered) })
		<-s.releaseStart
	}
	return nil
}

func (s *admissionRevocationMOServer) Stop() error {
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

type blockedStopTaskRunner struct {
	*testRunner
	stopEntered chan struct{}
	releaseStop chan struct{}
}

func (r *blockedStopTaskRunner) Stop() error {
	close(r.stopEntered)
	<-r.releaseStop
	return r.testRunner.Stop()
}

type admissionCNHAKeeperClient struct {
	logservice.CNHAKeeperClient
	id          uint64
	key         string
	deadline    time.Time
	hasDeadline bool
}

func (c *admissionCNHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	c.key = key
	c.deadline, c.hasDeadline = ctx.Deadline()
	return c.id, nil
}

func TestViewMetadataAdmissionWaitTimeoutUsesAuthoritativeOwnerExpiry(t *testing.T) {
	require.Equal(t, 30*time.Second, viewMetadataAdmissionWaitTimeout(0, nil))
	require.Equal(t, 5*time.Second,
		viewMetadataAdmissionWaitTimeout(5*time.Second, nil))
	require.Equal(t, 36*time.Second, viewMetadataAdmissionWaitTimeout(
		5*time.Second,
		&logservicepb.ViewMetadataAdmission{
			OwnerExpiryRemainingTicks: 301,
			TickPerSecond:             10,
		}))
}

func TestCNViewMetadataAdmissionGenerationLifecycle(t *testing.T) {
	serviceID := "cn-admission-generation-lifecycle"
	runtime.SetupServiceBasedRuntime(serviceID, runtime.DefaultRuntime())
	client := &admissionCNHAKeeperClient{id: 17}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = 5 * time.Second
	s := &service{
		cfg:             cfg,
		_hakeeperClient: client,
	}

	require.NoError(t, s.initViewMetadataAdmission(context.Background()))
	require.Equal(t, viewMetadataAdmissionGenerationKey, client.key)
	require.True(t, client.hasDeadline)
	require.Positive(t, time.Until(client.deadline))
	require.LessOrEqual(t, time.Until(client.deadline), 5*time.Second)
	require.Equal(t, uint64(17), s.viewMetadataAdmissionGeneration)
	value, ok := runtime.ServiceRuntime(serviceID).GetGlobalVariables(
		compile.ViewMetadataEpochFenceRuntimeKey)
	require.True(t, ok)
	require.Same(t, s.viewMetadataEpochFence, value)

	s.closeViewMetadataAdmission()
	_, ok = runtime.ServiceRuntime(serviceID).GetGlobalVariables(
		compile.ViewMetadataEpochFenceRuntimeKey)
	require.False(t, ok)
	_, err := s.viewMetadataEpochFence.Acquire(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}

func TestCNViewMetadataAdmissionFencesCatalogBeforeReady(t *testing.T) {
	var statements atomic.Int64
	s := &service{
		cfg:                             &Config{},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 7,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.sqlExecutor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		statements.Add(1)
		return executor.Result{}, nil
	})
	s.viewMetadataCatalogFenceReady.Store(true)
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Preparing:            true,
		Epoch:                3,
		RevalidationRequired: true,
		Generation:           7,
		Admitted:             true,
	})

	require.NoError(t, s.waitForViewMetadataAdmission())
	require.False(t, s.viewMetadataIngressReady.Load(),
		"admission alone must not publish CN ingress")
	require.Equal(t, uint64(3), s.viewMetadataEpochFence.Epoch())
	require.Equal(t, uint64(3), s.viewMetadataCatalogFencedEpoch.Load())
	require.Positive(t, statements.Load())
}

func TestCNViewMetadataAdmissionPublishesRefreshOnlyAfterCatalogFence(t *testing.T) {
	s := &service{
		viewMetadataAdmissionGeneration: 7,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	err := s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Generation:         7,
			Epoch:              3,
			CatalogFencedEpoch: 3,
			RefreshReady:       true,
			RefreshEnabled:     true,
		})
	require.NoError(t, err)
	require.Equal(t, uint64(3), s.viewMetadataCatalogFencedEpoch.Load())
	require.True(t, s.viewMetadataEpochFence.RefreshEnabled())

	lease, acquired, err := s.viewMetadataEpochFence.AcquireRefresh(context.Background())
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, uint64(3), lease.Epoch())
	lease.Release()
}

func TestCNViewMetadataAdmissionCanceledAdvanceKeepsRefreshSealed(t *testing.T) {
	fence := compile.NewViewMetadataEpochFence()
	require.NoError(t, fence.Advance(context.Background(), 1))
	require.True(t, fence.MarkCatalogFenced(1))
	require.True(t, fence.MarkRefreshReady(1))
	require.True(t, fence.EnableRefresh(1))
	blocker, err := fence.Acquire(context.Background())
	require.NoError(t, err)
	s := &service{
		viewMetadataAdmissionGeneration: 7,
		viewMetadataEpochFence:          fence,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	snapshot := &logservicepb.ViewMetadataAdmission{
		Generation:           7,
		Epoch:                2,
		RevalidationRequired: true,
		CatalogFencedEpoch:   2,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.applyViewMetadataAdmission(ctx, snapshot), context.Canceled)
	require.False(t, fence.RefreshEnabled())
	_, _, err = fence.AcquireRefresh(ctx)
	require.ErrorIs(t, err, context.Canceled)

	blocker.Release()
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), snapshot))
	require.Equal(t, uint64(2), fence.Epoch())
	require.False(t, fence.RefreshEnabled())
}

func TestCNViewMetadataAdmissionRejectsRefreshEnableBeforeCatalogFence(t *testing.T) {
	s := &service{
		viewMetadataAdmissionGeneration: 7,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	err := s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Generation:     7,
			Epoch:          3,
			RefreshReady:   true,
			RefreshEnabled: true,
		})
	require.Error(t, err)
	require.False(t, s.viewMetadataEpochFence.RefreshEnabled())
}

func TestCNViewMetadataAdmissionRejectsSupersededResponse(t *testing.T) {
	s := &service{
		cfg:                             &Config{},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 8,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true})
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      4,
			Generation: 7,
			Ready:      true,
		}))
	require.Zero(t, s.viewMetadataEpochFence.Epoch())
	require.False(t, s.viewMetadataAdmission.Load().Enabled)
}

func TestCNViewMetadataAdmissionRevokesIngressForHigherGeneration(t *testing.T) {
	mo := &admissionRevocationMOServer{stopped: make(chan struct{})}
	closeRequested := make(chan struct{})
	s := &service{
		cfg:                             &Config{UUID: "superseded-cn"},
		logger:                          zap.NewNop(),
		mo:                              mo,
		viewMetadataAdmissionGeneration: 8,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		viewMetadataCloseFn: func() error {
			_ = mo.Stop()
			close(closeRequested)
			return errors.New("expected close error")
		},
	}
	runner := &testRunner{}
	s.viewMetadataIngressReady.Store(true)
	s.task.runnerReady.Store(true)
	s.task.runner = runner
	pipelineCtx, releasePipeline, admitted := s.admitPipelineHandler(context.Background())
	require.True(t, admitted)
	releaseQuery, admitted := s.queryWork.admit()
	require.True(t, admitted)

	require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      4,
			Generation: 9,
		}))
	require.Equal(t, uint64(9), s.viewMetadataAdmission.Load().Generation)
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, s.task.runnerReady.Load(),
		"generation revocation must synchronously withdraw task-runner eligibility")
	require.Equal(t, 1, runner.stopped,
		"generation revocation must synchronously stop the running task runner")
	require.Nil(t, s.GetTaskRunner(), "the stopped task runner must be detached")
	select {
	case <-pipelineCtx.Done():
	default:
		t.Fatal("active pipeline was not canceled after generation revocation")
	}
	select {
	case <-closeRequested:
	case <-time.After(time.Second):
		t.Fatal("superseded CN did not request full service closure")
	}
	select {
	case <-mo.stopped:
	default:
		t.Fatal("serialized full closure did not stop SQL frontend")
	}
	_, _, admitted = s.admitPipelineHandler(context.Background())
	require.False(t, admitted)
	_, admitted = s.queryWork.admit()
	require.False(t, admitted)

	releasePipeline()
	releaseQuery()
}

func TestCNGenerationRevocationStopsFrontendBeforeTaskRunnerDrain(t *testing.T) {
	mo := &admissionRevocationMOServer{stopped: make(chan struct{})}
	runner := &blockedStopTaskRunner{
		testRunner:  &testRunner{},
		stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}),
	}
	s := &service{
		cfg:                             &Config{UUID: "task-runner-blocked-stop"},
		logger:                          zap.NewNop(),
		mo:                              mo,
		viewMetadataAdmissionGeneration: 8,
		viewMetadataCloseFn:             func() error { return nil },
	}
	s.task.runner = runner
	s.task.runnerReady.Store(true)

	revokeDone := make(chan struct{})
	go func() {
		s.revokeViewMetadataGeneration(9)
		close(revokeDone)
	}()
	select {
	case <-runner.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("generation revocation did not start draining the task runner")
	}
	select {
	case <-mo.stopped:
	case <-time.After(time.Second):
		t.Fatal("frontend was not stopped before the task runner drain blocked")
	}
	select {
	case <-revokeDone:
		t.Fatal("generation revocation returned before the task runner stopped")
	default:
	}
	close(runner.releaseStop)
	select {
	case <-revokeDone:
	case <-time.After(time.Second):
		t.Fatal("generation revocation did not finish after the task runner stopped")
	}
	require.Equal(t, 1, runner.stopped)
	require.Nil(t, s.GetTaskRunner())
}

func TestCNGenerationRevocationStopsTaskRunnerStartInFlight(t *testing.T) {
	s := &service{
		cfg:                             &Config{UUID: "task-runner-revoked-during-start"},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 8,
		viewMetadataCloseFn:             func() error { return nil },
	}
	runner := &testRunner{}

	// Model TaskRunner.Start in progress: it owns task.Lock and publishes the
	// runner immediately before releasing the lock. Revocation must wait for
	// that publication, then detach and stop the runner before returning.
	s.task.Lock()
	revokeDone := make(chan struct{})
	go func() {
		s.revokeViewMetadataGeneration(9)
		close(revokeDone)
	}()
	require.Eventually(t, s.viewMetadataGenerationRevoked.Load, time.Second, time.Millisecond)
	s.task.runner = runner
	s.task.runnerReady.Store(true)
	s.task.Unlock()

	select {
	case <-revokeDone:
	case <-time.After(time.Second):
		t.Fatal("generation revocation did not finish stopping the in-flight task runner")
	}
	require.Equal(t, 1, runner.stopped)
	require.False(t, s.task.runnerReady.Load())
	require.Nil(t, s.GetTaskRunner())
}

func TestCNGenerationRevocationCancelsIngressStart(t *testing.T) {
	mo := &admissionRevocationMOServer{
		stopped:      make(chan struct{}),
		startEntered: make(chan struct{}),
		releaseStart: make(chan struct{}),
	}
	closeRequested := make(chan struct{})
	s := &service{
		cfg:                             &Config{UUID: "revoked-during-start"},
		logger:                          zap.NewNop(),
		mo:                              mo,
		cancelMoServerFunc:              func() {},
		viewMetadataAdmissionGeneration: 8,
		viewMetadataCloseFn: func() error {
			close(closeRequested)
			return nil
		},
	}
	startDone := make(chan error, 1)
	go func() { startDone <- s.startFrontendUnlessViewMetadataGenerationRevoked() }()
	<-mo.startEntered
	revokeDone := make(chan struct{})
	go func() {
		s.revokeViewMetadataGeneration(9)
		close(revokeDone)
	}()
	require.Eventually(t, s.viewMetadataGenerationRevoked.Load, time.Second, time.Millisecond)
	require.False(t, s.viewMetadataIngressReady.Load())
	close(mo.releaseStart)

	err := <-startDone
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	<-revokeDone
	require.Equal(t, int32(1), mo.startCount.Load(), "revocation must not restart frontend")
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, s.task.runnerReady.Load())
	err = s.publishTaskRunner()
	require.Error(t, err, "a revoke between startup precheck and task publication must win")
	require.False(t, s.task.runnerReady.Load())
	select {
	case <-mo.stopped:
	default:
		t.Fatal("revocation did not stop frontend after the in-flight Start completed")
	}
	select {
	case <-closeRequested:
	case <-time.After(time.Second):
		t.Fatal("revocation did not request serialized full closure")
	}
}

func TestViewMetadataAuthorityLeaseDurationExpiresBeforeHAKeeper(t *testing.T) {
	require.Zero(t, viewMetadataAuthorityLeaseDuration(0, 10))
	require.Zero(t, viewMetadataAuthorityLeaseDuration(10, 0))
	require.Equal(t, time.Nanosecond, viewMetadataAuthorityLeaseDuration(1, 10))
	require.Equal(t, 30*time.Second,
		viewMetadataAuthorityLeaseDuration(301, 10))
}

func TestCNViewMetadataAuthorityExpirySealsOnlyMetadataIngress(t *testing.T) {
	mo := &admissionRevocationMOServer{stopped: make(chan struct{})}
	closeRequested := make(chan struct{})
	s := &service{
		cfg:                             &Config{UUID: "authority-partition"},
		logger:                          zap.NewNop(),
		mo:                              mo,
		viewMetadataAdmissionGeneration: 19,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataCloseFn: func() error {
			close(closeRequested)
			return nil
		},
	}
	s.viewMetadataIngressReady.Store(true)
	require.NoError(t, s.viewMetadataEpochFence.Advance(context.Background(), 1))
	require.True(t, s.viewMetadataEpochFence.MarkCatalogFenced(1))
	require.True(t, s.viewMetadataEpochFence.MarkRefreshReady(1))
	require.True(t, s.viewMetadataEpochFence.EnableRefresh(1))
	blockedReader, _, err := s.viewMetadataEpochFence.AcquireRefresh(context.Background())
	require.NoError(t, err)
	defer blockedReader.Release()

	s.renewViewMetadataAuthorityLease(&logservicepb.ViewMetadataAdmission{
		Generation:          19,
		RefreshEnabled:      true,
		AuthorityLeaseTicks: 301,
		TickPerSecond:       10,
	}, 0)
	s.viewMetadataAuthorityMu.Lock()
	version := s.viewMetadataAuthorityVersion
	s.viewMetadataAuthorityMu.Unlock()
	deadline, required, err := s.viewMetadataEpochFence.AuthorityDeadline()
	require.NoError(t, err)
	require.True(t, required)
	// Deterministically model a HAKeeper-only partition lasting through the
	// local authority deadline while the metadata reader remains blocked.
	s.expireViewMetadataAuthority(version, deadline)

	require.ErrorIs(t, blockedReader.ValidateAuthority(), context.Canceled)
	_, _, err = s.viewMetadataEpochFence.AcquireRefresh(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	ordinaryLease, err := s.viewMetadataEpochFence.Acquire(context.Background())
	require.NoError(t, err)
	ordinaryLease.Release()

	require.False(t, s.viewMetadataGenerationRevoked.Load())
	require.True(t, s.viewMetadataIngressReady.Load())
	select {
	case <-mo.stopped:
		t.Fatal("authority expiry stopped ordinary SQL ingress")
	default:
	}
	select {
	case <-closeRequested:
		t.Fatal("authority expiry requested full CN closure")
	default:
	}

	s.renewViewMetadataAuthorityLease(&logservicepb.ViewMetadataAdmission{
		Generation:          19,
		RefreshEnabled:      true,
		AuthorityLeaseTicks: 301,
		TickPerSecond:       10,
	}, 0)
	recovered, enabled, err := s.viewMetadataEpochFence.AcquireRefresh(context.Background())
	require.NoError(t, err)
	require.True(t, enabled)
	recovered.Release()
}

func TestCNViewMetadataAuthorityPartitionContainsAndRecoversMultipleCNs(t *testing.T) {
	const cnCount = 3
	services := make([]*service, 0, cnCount)
	versions := make([]uint64, 0, cnCount)
	deadlines := make([]time.Time, 0, cnCount)
	for index := range cnCount {
		s := &service{
			cfg:                             &Config{UUID: fmt.Sprintf("partition-cn-%d", index)},
			logger:                          zap.NewNop(),
			viewMetadataAdmissionGeneration: uint64(index + 1),
			viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		}
		s.viewMetadataIngressReady.Store(true)
		require.NoError(t, s.viewMetadataEpochFence.Advance(context.Background(), 1))
		require.True(t, s.viewMetadataEpochFence.MarkCatalogFenced(1))
		require.True(t, s.viewMetadataEpochFence.MarkRefreshReady(1))
		require.True(t, s.viewMetadataEpochFence.EnableRefresh(1))
		s.renewViewMetadataAuthorityLease(&logservicepb.ViewMetadataAdmission{
			Generation:          s.viewMetadataAdmissionGeneration,
			RefreshEnabled:      true,
			AuthorityLeaseTicks: 301,
			TickPerSecond:       10,
		}, 0)
		s.viewMetadataAuthorityMu.Lock()
		versions = append(versions, s.viewMetadataAuthorityVersion)
		s.viewMetadataAuthorityMu.Unlock()
		deadline, required, err := s.viewMetadataEpochFence.AuthorityDeadline()
		require.NoError(t, err)
		require.True(t, required)
		deadlines = append(deadlines, deadline)
		services = append(services, s)
	}

	// Model one HAKeeper partition reaching every CN's local deadline.
	for index, s := range services {
		s.expireViewMetadataAuthority(versions[index], deadlines[index])
		_, _, err := s.viewMetadataEpochFence.AcquireRefresh(context.Background())
		require.ErrorIs(t, err, context.Canceled)
		ordinary, err := s.viewMetadataEpochFence.Acquire(context.Background())
		require.NoError(t, err)
		ordinary.Release()
		require.True(t, s.viewMetadataIngressReady.Load())
		require.False(t, s.viewMetadataGenerationRevoked.Load())
	}

	// A successful heartbeat independently reopens metadata on every CN. A
	// delayed callback from the partitioned lease must not seal the renewal.
	for index, s := range services {
		s.renewViewMetadataAuthorityLease(&logservicepb.ViewMetadataAdmission{
			Generation:          s.viewMetadataAdmissionGeneration,
			RefreshEnabled:      true,
			AuthorityLeaseTicks: 301,
			TickPerSecond:       10,
		}, 0)
		s.expireViewMetadataAuthority(versions[index], deadlines[index])
		lease, enabled, err := s.viewMetadataEpochFence.AcquireRefresh(context.Background())
		require.NoError(t, err)
		require.True(t, enabled)
		lease.Release()
		s.viewMetadataAuthorityMu.Lock()
		if s.viewMetadataAuthorityTimer != nil {
			s.viewMetadataAuthorityTimer.Stop()
		}
		s.viewMetadataAuthorityMu.Unlock()
	}
}

func TestCNViewMetadataAdmissionWaitsForAuthoritativeResponse(t *testing.T) {
	s := &service{
		cfg:                             &Config{},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	done := make(chan error, 1)
	go func() {
		done <- s.waitForViewMetadataAdmission()
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("CN admission returned before an authoritative response: %v", err)
	default:
	}

	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), nil))
	require.NoError(t, <-done)
}

func TestCNViewMetadataAdmissionWaitsPastDiscoveryForOwnerExpiry(t *testing.T) {
	cfg := &Config{}
	cfg.HAKeeper.DiscoveryTimeout.Duration = 20 * time.Millisecond
	s := &service{
		cfg:                             cfg,
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 17,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:                   true,
		Generation:                17,
		OwnerExpiryRemainingTicks: 1,
		TickPerSecond:             1,
	})
	done := make(chan error, 1)
	go func() { done <- s.waitForViewMetadataAdmission() }()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("CN admission ignored authoritative owner expiry: %v", err)
	default:
	}
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:    true,
		Generation: 17,
		Admitted:   true,
	})
	s.viewMetadataAdmissionUpdated <- struct{}{}
	require.NoError(t, <-done)
}

func TestCNViewMetadataAdmissionDoesNotAckFailedCatalogFence(t *testing.T) {
	fenceErr := errors.New("catalog fence failed")
	s := &service{
		cfg:                             &Config{},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 13,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.sqlExecutor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, fenceErr
	})
	s.viewMetadataCatalogFenceReady.Store(true)

	err := s.applyViewMetadataAdmission(context.Background(),
		&logservicepb.ViewMetadataAdmission{
			Enabled:              true,
			Epoch:                6,
			RevalidationRequired: true,
			Generation:           13,
		})
	require.ErrorIs(t, err, fenceErr)
	require.Equal(t, uint64(6), s.viewMetadataEpochFence.Epoch())
	require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
}
