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
	s.viewMetadataIngressReady.Store(true)
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
