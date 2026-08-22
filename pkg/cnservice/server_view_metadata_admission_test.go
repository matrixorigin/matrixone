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
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

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
		Ready:                true,
	})

	require.NoError(t, s.waitForViewMetadataAdmission())
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
