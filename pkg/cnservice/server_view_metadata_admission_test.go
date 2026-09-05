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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
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

func viewMetadataLifecycleGateTestResult() executor.Result {
	result := executor.NewMemResult(nil, nil)
	result.NewBatchWithRowCount(1)
	return result.GetResult()
}

type admissionRollbackJoiningExecutor struct {
	executor.SQLExecutor
}

func (e *admissionRollbackJoiningExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return errors.Join(e.SQLExecutor.ExecTxn(ctx, execFunc, opts), nil)
}

type admissionStartLockService struct {
	lockservice.LockService
}

func (*admissionStartLockService) Close() error {
	return nil
}

type admissionStartQueryService struct {
	queryservice.QueryService
}

func (*admissionStartQueryService) Start() error {
	return nil
}

func (*admissionStartQueryService) Close() error {
	return nil
}

func newViewMetadataAdmissionStartService(
	t *testing.T,
	boot *testBootService,
	sqlExecutor executor.SQLExecutor,
	discoveryTimeout time.Duration,
) *service {
	t.Helper()
	serviceID := strings.ReplaceAll(t.Name(), "/", "-")
	runtime.SetupServiceBasedRuntime(serviceID, runtime.DefaultRuntime())
	cfg := &Config{UUID: serviceID, AutomaticUpgrade: true}
	cfg.HAKeeper.DiscoveryTimeout.Duration = discoveryTimeout
	cfg.Txn.Trace.BufferSize = 1
	s := &service{
		cfg:                             cfg,
		logger:                          zap.NewNop(),
		server:                          closeOnlyRPCServer{},
		mo:                              closeErrorMOServer{},
		cancelMoServerFunc:              func() {},
		lockService:                     &admissionStartLockService{},
		queryService:                    &admissionStartQueryService{},
		sqlExecutor:                     sqlExecutor,
		bootstrapService:                boot,
		stopper:                         stopper.NewStopper("view-metadata-admission-start"),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.options.traceDataPath = t.TempDir()
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Epoch:                5,
		RevalidationRequired: true,
		Generation:           11,
		Admitted:             true,
	})
	return s
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
	s.sqlExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		statements.Add(1)
		if sql == catalog.ViewMetadataLifecycleGateSQL {
			return viewMetadataLifecycleGateTestResult(), nil
		}
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

func TestCNViewMetadataCatalogFenceShortcuts(t *testing.T) {
	snapshot := &logservicepb.ViewMetadataAdmission{
		Epoch:                3,
		RevalidationRequired: true,
	}

	t.Run("authority already fenced", func(t *testing.T) {
		s := &service{}
		copy := *snapshot
		copy.CatalogFencedEpoch = copy.Epoch
		require.NoError(t, s.fenceViewMetadataCatalog(context.Background(), &copy))
		require.Equal(t, copy.Epoch, s.viewMetadataCatalogFencedEpoch.Load())
	})

	t.Run("executor not ready", func(t *testing.T) {
		s := &service{}
		require.NoError(t, s.fenceViewMetadataCatalog(context.Background(), snapshot))
		require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
	})
}

func TestViewMetadataCatalogFenceRetryable(t *testing.T) {
	missingTable := moerr.NewNoSuchTableNoCtx("mo_catalog", "t")
	missingDatabase := moerr.NewBadDBNoCtx("mo_catalog")
	txnRetry := moerr.NewTxnNeedRetryNoCtx()
	txnRetryWithDefChanged := moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	rollbackErr := errors.New("rollback failed")
	tests := []struct {
		name               string
		err                error
		upgradeOwnerActive bool
		want               bool
	}{
		{name: "missing table without owner", err: missingTable, want: true},
		{name: "missing database without owner", err: missingDatabase, want: true},
		{name: "rollback joined missing table", err: errors.Join(missingTable, nil), want: true},
		{name: "wrapped rollback join", err: fmt.Errorf("transaction failed: %w", errors.Join(missingDatabase, nil)), want: true},
		{name: "deadline without owner", err: context.DeadlineExceeded, want: false},
		{name: "deadline with owner", err: context.DeadlineExceeded, upgradeOwnerActive: true, want: true},
		{name: "backend unavailable without owner", err: moerr.NewBackendCannotConnectNoCtx("departed lock owner"), want: true},
		{name: "txn retry without owner", err: txnRetry, want: false},
		{name: "txn retry with owner", err: txnRetry, upgradeOwnerActive: true, want: true},
		{
			name:               "txn retry with definition change and owner",
			err:                fmt.Errorf("fence failed: %w", txnRetryWithDefChanged),
			upgradeOwnerActive: true,
			want:               true,
		},
		{name: "owner cancellation", err: context.Canceled, upgradeOwnerActive: true, want: false},
		{name: "other failure", err: errors.New("executor failed"), upgradeOwnerActive: true, want: false},
		{
			name:               "readiness plus rollback failure",
			err:                errors.Join(missingTable, rollbackErr),
			upgradeOwnerActive: true,
			want:               false,
		},
		{
			name:               "txn retry plus rollback failure",
			err:                errors.Join(txnRetry, rollbackErr),
			upgradeOwnerActive: true,
			want:               false,
		},
		{
			name: "backend unavailable plus rollback failure",
			err: errors.Join(
				moerr.NewBackendCannotConnectNoCtx("departed lock owner"),
				rollbackErr,
			),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want,
				viewMetadataCatalogFenceRetryable(test.err, test.upgradeOwnerActive))
		})
	}
}

func TestCNViewMetadataAdmissionDefersCatalogFenceWhileUpgradePending(t *testing.T) {
	tests := []struct {
		name               string
		refreshGatePresent bool
	}{
		{name: "neither table exists"},
		{name: "only dependencies exists"},
		{name: "only refresh exists", refreshGatePresent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statements atomic.Int64
			s := &service{
				cfg:                             &Config{},
				logger:                          zap.NewNop(),
				viewMetadataAdmissionGeneration: 7,
				viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
				viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
			}
			s.sqlExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				statements.Add(1)
				if sql == catalog.ViewMetadataLifecycleGateSQL {
					if test.refreshGatePresent {
						return viewMetadataLifecycleGateTestResult(), nil
					}
					return executor.Result{}, nil
				}
				return executor.Result{}, moerr.NewNoSuchTableNoCtx(
					"mo_catalog", "mo_view_dependencies")
			})
			s.viewMetadataCatalogFenceReady.Store(true)

			require.NoError(t, s.applyViewMetadataAdmission(context.Background(),
				&logservicepb.ViewMetadataAdmission{
					Enabled:              true,
					Epoch:                3,
					RevalidationRequired: true,
					Generation:           7,
					Admitted:             true,
				}))
			require.Positive(t, statements.Load())
			require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load(),
				"an incomplete lifecycle catalog must never be acknowledged as fenced")
		})
	}
}

func TestCNViewMetadataAdmissionWaitsForCatalogUpgradeCommit(t *testing.T) {
	const cnCount = 2
	catalogCommitted := &atomic.Bool{}
	releasePendingAttempts := make(chan struct{})
	var releaseOnce sync.Once
	releasePending := func() { releaseOnce.Do(func() { close(releasePendingAttempts) }) }
	t.Cleanup(releasePending)
	attempted := make([]chan struct{}, cnCount)
	services := make([]*service, cnCount)
	done := make(chan error, cnCount)

	for i := range cnCount {
		attempted[i] = make(chan struct{})
		var attemptOnce sync.Once
		s := &service{
			cfg:                             &Config{UUID: fmt.Sprintf("catalog-upgrade-cn-%d", i)},
			logger:                          zap.NewNop(),
			viewMetadataAdmissionGeneration: 11,
			viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
			viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		}
		s.cfg.HAKeeper.DiscoveryTimeout.Duration = 5 * time.Second
		s.sqlExecutor = &admissionRollbackJoiningExecutor{
			SQLExecutor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if !catalogCommitted.Load() {
					attemptOnce.Do(func() { close(attempted[i]) })
					<-releasePendingAttempts
					return executor.Result{}, nil
				}
				if sql == catalog.ViewMetadataLifecycleGateSQL {
					return viewMetadataLifecycleGateTestResult(), nil
				}
				return executor.Result{}, nil
			}),
		}
		s.viewMetadataCatalogFenceReady.Store(true)
		s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
			Enabled:              true,
			Epoch:                5,
			RevalidationRequired: true,
			Generation:           11,
			Admitted:             true,
		})
		services[i] = s
		go func() { done <- s.waitForViewMetadataAdmission() }()
	}

	for i := range cnCount {
		select {
		case <-attempted[i]:
		case <-time.After(time.Second):
			t.Fatalf("CN %d did not reach the pre-upgrade catalog fence", i)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("CN admission completed before the catalog upgrade committed: %v", err)
	default:
	}

	catalogCommitted.Store(true)
	releasePending()
	for range cnCount {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("CN admission did not retry after the catalog upgrade committed")
		}
	}
	for i, s := range services {
		require.Equal(t, uint64(5), s.viewMetadataCatalogFencedEpoch.Load(),
			"CN %d did not durably fence the upgraded catalog", i)
	}
}

func TestCNViewMetadataAdmissionRejectsStaleFencedSnapshot(t *testing.T) {
	oldFenceEntered := make(chan struct{})
	newFenceEntered := make(chan struct{})
	releaseOldFence := make(chan struct{})
	releaseNewFence := make(chan struct{})
	var releaseOldOnce sync.Once
	var releaseNewOnce sync.Once
	releaseOld := func() { releaseOldOnce.Do(func() { close(releaseOldFence) }) }
	releaseNew := func() { releaseNewOnce.Do(func() { close(releaseNewFence) }) }
	t.Cleanup(func() {
		releaseOld()
		releaseNew()
	})

	var gateAttempts atomic.Int64
	s := &service{
		cfg:                             &Config{UUID: "stale-fenced-snapshot"},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.cfg.HAKeeper.DiscoveryTimeout.Duration = 5 * time.Second
	s.sqlExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == catalog.ViewMetadataLifecycleGateSQL {
			switch gateAttempts.Add(1) {
			case 1:
				close(oldFenceEntered)
				<-releaseOldFence
			case 2:
				close(newFenceEntered)
				<-releaseNewFence
			}
			return viewMetadataLifecycleGateTestResult(), nil
		}
		return executor.Result{}, nil
	})
	s.viewMetadataCatalogFenceReady.Store(true)
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Epoch:                5,
		RevalidationRequired: true,
		Generation:           11,
		Admitted:             true,
	})

	done := make(chan error, 1)
	go func() { done <- s.waitForViewMetadataAdmission() }()
	select {
	case <-oldFenceEntered:
	case <-time.After(time.Second):
		t.Fatal("startup waiter did not begin fencing the old epoch")
	}

	newSnapshot := &logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Epoch:                6,
		RevalidationRequired: true,
		Generation:           11,
		Admitted:             false,
	}
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), newSnapshot))
	releaseOld()
	select {
	case err := <-done:
		t.Fatalf("startup accepted the stale admitted snapshot: %v", err)
	case <-newFenceEntered:
	}
	require.Equal(t, uint64(5), s.viewMetadataCatalogFencedEpoch.Load())
	require.Equal(t, uint64(6), s.viewMetadataAdmission.Load().Epoch)
	require.False(t, s.viewMetadataAdmission.Load().Admitted)

	newSnapshot.Admitted = true
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), newSnapshot))
	releaseNew()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("startup did not accept the current fenced and admitted epoch")
	}
	require.Equal(t, uint64(6), s.viewMetadataCatalogFencedEpoch.Load())
}

func TestCNViewMetadataAdmissionLinearizesFinalIngressHandoff(t *testing.T) {
	handoffValidated := make(chan struct{})
	releaseHandoff := make(chan struct{})
	newFenceEntered := make(chan struct{})
	releaseNewFence := make(chan struct{})
	var releaseHandoffOnce sync.Once
	var releaseNewFenceOnce sync.Once
	releaseValidatedHandoff := func() { releaseHandoffOnce.Do(func() { close(releaseHandoff) }) }
	releaseFence := func() { releaseNewFenceOnce.Do(func() { close(releaseNewFence) }) }
	t.Cleanup(func() {
		releaseValidatedHandoff()
		releaseFence()
	})

	s := &service{
		cfg:                             &Config{UUID: "linearized-admission-handoff"},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.cfg.HAKeeper.DiscoveryTimeout.Duration = 5 * time.Second
	s.sqlExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == catalog.ViewMetadataLifecycleGateSQL {
			close(newFenceEntered)
			<-releaseNewFence
			return viewMetadataLifecycleGateTestResult(), nil
		}
		return executor.Result{}, nil
	})
	s.viewMetadataCatalogFenceReady.Store(true)
	require.NoError(t, s.viewMetadataEpochFence.Advance(context.Background(), 5))
	s.viewMetadataCatalogFencedEpoch.Store(5)
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Epoch:                5,
		RevalidationRequired: true,
		Generation:           11,
		Admitted:             true,
	})
	var handoffOnce sync.Once
	s.beforeViewMetadataAdmissionHandoff = func() {
		handoffOnce.Do(func() { close(handoffValidated) })
		<-releaseHandoff
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- s.waitForViewMetadataIngressAdmission() }()
	select {
	case <-handoffValidated:
	case <-time.After(time.Second):
		t.Fatal("startup did not reach final admission handoff")
	}

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- s.applyViewMetadataAdmission(context.Background(), &logservicepb.ViewMetadataAdmission{
			Enabled:              true,
			Epoch:                6,
			RevalidationRequired: true,
			Generation:           11,
			Admitted:             false,
		})
	}()
	require.Eventually(t, func() bool {
		return s.viewMetadataEpochFence.Epoch() == 6 &&
			s.viewMetadataAdmissionMuWaiters.Load() == 1
	}, time.Second, time.Millisecond, "updater did not block acquiring the snapshot publication mutex")
	select {
	case err := <-applyDone:
		t.Fatalf("new snapshot publication bypassed the final handoff: %v", err)
	default:
	}
	require.Equal(t, uint64(5), s.viewMetadataAdmission.Load().Epoch)

	releaseValidatedHandoff()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("final admission handoff did not complete")
	}
	require.True(t, s.viewMetadataIngressReady.Load())
	select {
	case <-newFenceEntered:
	case <-time.After(time.Second):
		t.Fatal("post-handoff snapshot skipped its catalog fence")
	}
	require.Equal(t, uint64(6), s.viewMetadataAdmission.Load().Epoch)
	require.False(t, s.viewMetadataCatalogFenceStartupWaiting.Load())

	releaseFence()
	select {
	case err := <-applyDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("post-handoff snapshot did not finish catalog fencing")
	}
	require.Equal(t, uint64(6), s.viewMetadataCatalogFencedEpoch.Load())
}

func TestViewMetadataCatalogFenceRetryDelayIsBoundedAndJittered(t *testing.T) {
	base := viewMetadataCatalogFenceInitialRetryDelay
	var previous time.Duration
	for attempt := uint32(0); attempt < 20; attempt++ {
		delay := viewMetadataCatalogFenceRetryDelay("cn-a", attempt)
		require.GreaterOrEqual(t, delay, base*4/5)
		require.LessOrEqual(t, delay, base*6/5)
		require.LessOrEqual(t, delay, viewMetadataCatalogFenceMaxRetryDelay)
		if attempt <= 4 {
			require.Greater(t, delay, previous)
		}
		previous = delay
		if base < viewMetadataCatalogFenceMaxRetryDelay/2 {
			base *= 2
		}
	}
	require.NotEqual(t,
		viewMetadataCatalogFenceRetryDelay("cn-a", 0),
		viewMetadataCatalogFenceRetryDelay("cn-b", 0))
}

func TestServiceStartWaitsForCatalogUpgradePastDiscoveryDeadline(t *testing.T) {
	const discoveryTimeout = 20 * time.Millisecond
	var catalogCommitted atomic.Bool
	var catalogStatements atomic.Int64
	catalogAttempted := make(chan struct{})
	var attemptOnce sync.Once
	boot := &testBootService{}
	sqlExecutor := &admissionRollbackJoiningExecutor{
		SQLExecutor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			catalogStatements.Add(1)
			if sql == catalog.ViewMetadataLifecycleGateSQL {
				if !catalogCommitted.Load() {
					attemptOnce.Do(func() { close(catalogAttempted) })
					return executor.Result{}, nil
				}
				return viewMetadataLifecycleGateTestResult(), nil
			}
			return executor.Result{}, nil
		}),
	}
	s := newViewMetadataAdmissionStartService(t, boot, sqlExecutor, discoveryTimeout)
	startDone := make(chan error, 1)
	go func() { startDone <- s.Start() }()
	t.Cleanup(func() {
		catalogCommitted.Store(true)
		s.notifyViewMetadataAdmissionUpdated()
		_ = s.Close()
	})

	select {
	case <-catalogAttempted:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach the pre-upgrade catalog fence")
	}
	for range 3 {
		require.NoError(t, s.applyViewMetadataAdmission(context.Background(), s.viewMetadataAdmission.Load()))
	}
	select {
	case err := <-startDone:
		t.Fatalf("Start returned before the upgrade owner deadline: %v", err)
	case <-time.After(2 * discoveryTimeout):
	}
	require.Equal(t, int64(1), catalogStatements.Load(),
		"heartbeat updates must not bypass the startup catalog backoff")

	catalogCommitted.Store(true)
	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not converge after the catalog upgrade committed")
	}
	require.True(t, s.viewMetadataIngressReady.Load())
	require.Equal(t, uint64(5), s.viewMetadataCatalogFencedEpoch.Load())
}

func TestServiceStartRetriesTransientCatalogFenceErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "backend unavailable", err: moerr.NewBackendCannotConnectNoCtx("departed lock owner")},
		{name: "txn retry", err: moerr.NewTxnNeedRetryNoCtx()},
		{name: "txn retry with definition change", err: moerr.NewTxnNeedRetryWithDefChangedNoCtx()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const discoveryTimeout = 20 * time.Millisecond
			var catalogReady atomic.Bool
			catalogAttempted := make(chan struct{})
			var attemptOnce sync.Once
			sqlExecutor := &admissionRollbackJoiningExecutor{
				SQLExecutor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					if sql == catalog.ViewMetadataLifecycleGateSQL {
						if !catalogReady.Load() {
							attemptOnce.Do(func() { close(catalogAttempted) })
							return executor.Result{}, test.err
						}
						return viewMetadataLifecycleGateTestResult(), nil
					}
					return executor.Result{}, nil
				}),
			}
			s := newViewMetadataAdmissionStartService(t, &testBootService{}, sqlExecutor, discoveryTimeout)
			startDone := make(chan error, 1)
			go func() { startDone <- s.Start() }()
			t.Cleanup(func() {
				catalogReady.Store(true)
				s.notifyViewMetadataAdmissionUpdated()
				_ = s.Close()
			})

			select {
			case <-catalogAttempted:
			case <-time.After(time.Second):
				t.Fatal("Start did not execute the catalog fence")
			}
			select {
			case err := <-startDone:
				t.Fatalf("transient catalog fence error terminated Start: %v", err)
			case <-time.After(2 * discoveryTimeout):
			}

			catalogReady.Store(true)
			select {
			case err := <-startDone:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("Start did not retry the transient catalog fence error")
			}
			require.True(t, s.viewMetadataIngressReady.Load())
			require.Equal(t, uint64(5), s.viewMetadataCatalogFencedEpoch.Load())
		})
	}
}

func TestServiceStartRejectsMixedCatalogFenceRollbackFailure(t *testing.T) {
	rollbackErr := errors.New("catalog fence rollback failed")
	mixedErr := errors.Join(moerr.NewTxnNeedRetryNoCtx(), rollbackErr)
	catalogAttempted := make(chan struct{})
	var attemptOnce sync.Once
	sqlExecutor := &admissionRollbackJoiningExecutor{
		SQLExecutor: executor.NewMemExecutor(func(string) (executor.Result, error) {
			attemptOnce.Do(func() { close(catalogAttempted) })
			return executor.Result{}, mixedErr
		}),
	}
	s := newViewMetadataAdmissionStartService(t, &testBootService{}, sqlExecutor, time.Second)
	startDone := make(chan error, 1)
	go func() { startDone <- s.Start() }()
	t.Cleanup(func() { _ = s.Close() })

	select {
	case <-catalogAttempted:
	case <-time.After(time.Second):
		t.Fatal("Start did not execute the catalog fence")
	}
	select {
	case err := <-startDone:
		require.ErrorIs(t, err, rollbackErr)
	case <-time.After(time.Second):
		t.Fatal("mixed rollback failure did not fail closed")
	}
	require.False(t, s.viewMetadataIngressReady.Load())
	require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
}

func TestServiceStartPropagatesBootstrapUpgradeFailure(t *testing.T) {
	upgradeErr := errors.New("cluster catalog upgrade failed")
	upgradeEntered := make(chan struct{})
	releaseUpgrade := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseUpgrade) }) }
	boot := &testBootService{
		bootstrapUpgradeHook: func(context.Context) error {
			close(upgradeEntered)
			<-releaseUpgrade
			return upgradeErr
		},
	}
	catalogAttempted := make(chan struct{})
	var attemptOnce sync.Once
	sqlExecutor := &admissionRollbackJoiningExecutor{
		SQLExecutor: executor.NewMemExecutor(func(string) (executor.Result, error) {
			attemptOnce.Do(func() { close(catalogAttempted) })
			return executor.Result{}, nil
		}),
	}
	s := newViewMetadataAdmissionStartService(t, boot, sqlExecutor, time.Second)
	startDone := make(chan error, 1)
	go func() { startDone <- s.Start() }()
	t.Cleanup(func() {
		release()
		_ = s.Close()
	})

	select {
	case <-upgradeEntered:
	case <-time.After(time.Second):
		t.Fatal("automatic bootstrap upgrade did not start")
	}
	select {
	case <-catalogAttempted:
	case <-time.After(time.Second):
		t.Fatal("Start did not wait on the incomplete catalog")
	}
	release()

	select {
	case err := <-startDone:
		require.ErrorIs(t, err, upgradeErr)
		require.NotContains(t, err.Error(), "was not admitted before startup deadline")
	case <-time.After(time.Second):
		t.Fatal("Start did not propagate the automatic upgrade failure")
	}
	require.False(t, s.viewMetadataIngressReady.Load())
	require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
}

func TestServiceStartRejectsBufferedBootstrapUpgradeFailure(t *testing.T) {
	upgradeErr := errors.New("cluster upgrade failed after catalog commit")
	upgradeReturned := make(chan struct{})
	boot := &testBootService{
		bootstrapUpgradeHook: func(context.Context) error {
			close(upgradeReturned)
			return upgradeErr
		},
	}
	var s *service
	sqlExecutor := &admissionRollbackJoiningExecutor{
		SQLExecutor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			if sql != catalog.ViewMetadataLifecycleGateSQL {
				return executor.Result{}, nil
			}
			<-upgradeReturned
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for len(s.bootstrapUpgradeResult) == 0 {
				select {
				case <-deadline.C:
					return executor.Result{}, errors.New("upgrade result was not published")
				case <-time.After(time.Millisecond):
				}
			}
			return viewMetadataLifecycleGateTestResult(), nil
		}),
	}
	s = newViewMetadataAdmissionStartService(t, boot, sqlExecutor, time.Second)
	t.Cleanup(func() { _ = s.Close() })

	err := s.Start()
	require.ErrorIs(t, err, upgradeErr)
	require.False(t, s.viewMetadataIngressReady.Load())
}

func TestCNViewMetadataAdmissionWithoutUpgradeOwnerIsBounded(t *testing.T) {
	s := &service{
		cfg:                             &Config{UUID: "catalog-upgrade-timeout"},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.cfg.HAKeeper.DiscoveryTimeout.Duration = 50 * time.Millisecond
	s.sqlExecutor = executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, moerr.NewNoSuchTableNoCtx(
			"mo_catalog", "mo_view_dependencies")
	})
	s.viewMetadataCatalogFenceReady.Store(true)
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Epoch:                5,
		RevalidationRequired: true,
		Generation:           11,
		Admitted:             true,
	})

	err := s.waitForViewMetadataAdmission()
	require.ErrorContains(t, err, "was not admitted before startup deadline")
	require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
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
