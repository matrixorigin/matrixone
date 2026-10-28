// Copyright 2026 MatrixOne
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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

// TestCNViewMetadataSameGenerationFutureFloorPoisonsWithoutRevocation pins
// design class (iii-b) of
// docs/design/20261027-implicit-boolean-semantics-enum-set-bit-json.md
// (revision 7): an already-serving CN that receives a same-generation future
// floor (a raised persisted-expression protocol it cannot satisfy) must fail
// CLOSED as a typed NotSupported, poison its published snapshot, and clear
// only its own authoring floor. It must NOT revoke the generation: ingress
// keeps listening, the task runner keeps admitting, the SQL frontend keeps
// serving established sessions, and the durable read floor keeps its last
// accepted value. Revocation requires a generation change, whose shutdown
// behavior is pinned by TestCNViewMetadataAdmissionRevokesIngressForHigherGeneration.
func TestCNViewMetadataSameGenerationFutureFloorPoisonsWithoutRevocation(t *testing.T) {
	serviceID := "cn-admission-same-generation-future-floor"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(serviceID, rt)
	t.Cleanup(func() {
		for _, name := range []string{
			runtime.PersistedExpressionProtocolFloor,
			runtime.PersistedExpressionProtocolAuthoringFloor,
		} {
			if value, ok := rt.GetGlobalVariables(name); ok {
				rt.CompareAndDeleteGlobalVariables(name, value)
			}
		}
	})
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	rt.SetGlobalVariables(runtime.PersistedExpressionProtocolFloor, int64(0))
	rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, int64(0))

	mo := &admissionRevocationMOServer{stopped: make(chan struct{})}
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		logger:                          zap.NewNop(),
		mo:                              mo,
		viewMetadataAdmissionGeneration: 9,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
	}
	s.viewMetadataIngressReady.Store(true)
	s.task.runnerReady.Store(true)
	runner := &testRunner{}
	s.task.runner = runner
	pipelineCtx, releasePipeline, admitted := s.admitPipelineHandler(context.Background())
	require.True(t, admitted)
	defer releasePipeline()
	releaseQuery, admitted := s.queryWork.admit()
	require.True(t, admitted)
	defer releaseQuery()

	// This CN is serving and admitted at the current floor, with the read and
	// authoring gates published for the old protocol.
	require.NoError(t, s.applyViewMetadataAdmission(context.Background(), &logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Ready:                true,
		Admitted:             true,
		Epoch:                2,
		Generation:           9,
		RevalidationRequired: true,
		CatalogFencedEpoch:   2,
		PersistedExpressionRequiredProtocolVersion: uint64(defines.MORPCVersion72),
	}))
	readFloor, ok := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolFloor)
	require.True(t, ok)
	require.Equal(t, int64(defines.MORPCVersion72), readFloor)
	authoringFloor, ok := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	require.True(t, ok)
	require.Equal(t, int64(defines.MORPCVersion72), authoringFloor)
	require.True(t, s.viewMetadataIngressReady.Load())

	// The same-generation future floor arrives: this build cannot satisfy it.
	futureVersion := uint64(defines.MORPCLatestVersion) + 1
	err := s.applyViewMetadataAdmission(context.Background(), &logservicepb.ViewMetadataAdmission{
		Enabled:              true,
		Ready:                true,
		Admitted:             true,
		Epoch:                3,
		Generation:           9,
		RevalidationRequired: true,
		CatalogFencedEpoch:   3,
		PersistedExpressionRequiredProtocolVersion: futureVersion,
	})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
		"the same-generation future floor must fail closed with a typed NotSupported")
	require.ErrorContains(t, err, "persisted expression protocol version")

	// The published snapshot is poisoned closed for this same generation.
	stored := s.viewMetadataAdmission.Load()
	require.True(t, stored.Preparing,
		"the poisoned snapshot must be Preparing")
	require.False(t, stored.Enabled)
	require.False(t, stored.Ready)
	require.False(t, stored.Admitted)
	require.Equal(t, uint64(9), stored.Generation)

	// The epoch is not advanced and the durable read floor keeps its last
	// accepted value: a rejected floor raise publishes nothing new.
	require.Equal(t, uint64(2), s.viewMetadataEpochFence.Epoch(),
		"the rejected epoch-3 snapshot must not advance the epoch fence")
	readFloor, ok = rt.GetGlobalVariables(runtime.PersistedExpressionProtocolFloor)
	require.True(t, ok)
	require.Equal(t, int64(defines.MORPCVersion72), readFloor,
		"the rejected future floor must not publish a read floor")

	// Only the authoring floor is cleared.
	authoringFloor, ok = rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	require.True(t, ok)
	require.Zero(t, authoringFloor,
		"the rejection clears exactly the authoring floor")

	// No revocation: ingress, task runner and frontend stay in place. The
	// shutdown evidence that would close `stopped` belongs to generation
	// revocation alone.
	require.True(t, s.viewMetadataIngressReady.Load(),
		"the same-generation poison must keep serving ingress")
	require.False(t, s.viewMetadataGenerationRevoked.Load(),
		"the same-generation poison must not revoke the generation")
	require.True(t, s.task.runnerReady.Load())
	require.NotNil(t, s.GetTaskRunner())
	require.Zero(t, runner.stopped,
		"the same-generation poison must not stop the task runner")
	select {
	case <-pipelineCtx.Done():
		t.Fatal("same-generation future floor must not cancel active pipelines")
	default:
	}
	select {
	case <-mo.stopped:
		t.Fatal("same-generation future floor must not stop the SQL frontend")
	default:
	}
}
