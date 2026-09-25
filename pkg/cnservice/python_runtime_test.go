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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/stretchr/testify/require"
)

func TestBuildCNRuntimeDegradesPythonWhenArtifactServiceIsUnavailable(t *testing.T) {
	cfg := python.ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:50051",
	}
	runtime, err := buildCNRuntime(cfg, nil, nil)
	require.NoError(t, err)

	readiness, ok := runtime.(udf.RuntimeReadiness)
	require.True(t, ok)
	require.ErrorContains(t, readiness.CheckLanguageReady(context.Background(), udf.LanguagePython), "RESOURCE_UNAVAILABLE")

	status, ok := runtime.(udf.RuntimeStatusProvider)
	require.True(t, ok)
	snapshot := status.StatusSnapshot(context.Background(), udf.LanguagePython)
	require.True(t, snapshot.Enabled)
	require.False(t, snapshot.Ready)
	require.Equal(t, udf.RuntimeStatusUnavailable, snapshot.ErrorClass)
}

func TestBuildCNRuntimeDisabledDoesNotCreatePythonRuntime(t *testing.T) {
	runtime, err := buildCNRuntime(python.ClientConfig{}, nil, nil)
	require.NoError(t, err)
	readiness, ok := runtime.(udf.RuntimeReadiness)
	require.True(t, ok)
	require.ErrorContains(t, readiness.CheckLanguageReady(context.Background(), udf.LanguagePython), "not enabled")
}

func TestUnavailablePythonRuntimeRejectsOperationsWithoutAWorker(t *testing.T) {
	runtime := &unavailablePythonRuntime{}
	require.Equal(t, udf.LanguagePython, runtime.Language())
	require.ErrorContains(t, runtime.Execute(
		context.Background(), nil, nil, nil,
	), "Python UDF runtime initialization failed")
	require.ErrorContains(t, runtime.CheckLanguageReady(context.Background(), "sql"), "unsupported readiness language")
	require.ErrorContains(t, runtime.CheckLanguageReady(context.Background(), udf.LanguagePython), "RESOURCE_UNAVAILABLE")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, runtime.CheckLanguageReady(canceled, udf.LanguagePython), context.Canceled)
	require.ErrorContains(t, runtime.ValidateDefinition(context.Background(), nil), "invalid Python definition")
	require.ErrorContains(t, runtime.ValidateDefinition(context.Background(), &udf.RoutineDefinition{Language: "sql"}), "invalid Python definition")
	require.Equal(t, udf.RuntimeStatusUnavailable,
		runtime.StatusSnapshot(context.Background(), "sql").ErrorClass)
}

type pythonStatusRuntime struct {
	udf.Runtime
	snapshot udf.RuntimeStatusSnapshot
}

func (r *pythonStatusRuntime) StatusSnapshot(context.Context, string) udf.RuntimeStatusSnapshot {
	return r.snapshot
}

type pythonRuntimeWithoutStatus struct{ udf.Runtime }

func TestGetPythonUdfStatusReturnsCNLocalBoundedRuntimeState(t *testing.T) {
	status := udf.RuntimeStatusSnapshot{
		Language:                   udf.LanguagePython,
		Enabled:                    true,
		AllowUnisolated:            true,
		Ready:                      true,
		ProtocolVersion:            1,
		ABIContract:                udf.PythonABIContract,
		AdapterVersion:             udf.PythonAdapterVersion,
		SDKVersion:                 udf.PythonSDKVersion,
		DefinitionSchemaVersion:    1,
		PlanContractVersion:        1,
		TypeDescriptorContract:     "arrow-sql-v1",
		TimezoneDatabaseVersion:    "2026c",
		WindowBatches:              1,
		MaxExecutionFrameBytes:     1 << 30,
		MaxHandlerProcesses:        8,
		MaxAccountHandlerProcesses: 8,
		MaxOwnerHandlerProcesses:   4,
		LeaseEpoch:                 42,
		Modes:                      []string{"SCALAR", "VECTOR"},
		NullPolicies:               []string{"CALLED_ON_NULL_INPUT", "RETURNS_NULL_ON_NULL_INPUT"},
	}
	s := &service{
		metadata:   metadata.CNStore{UUID: "cn-local"},
		udfService: &pythonStatusRuntime{Runtime: &unavailablePythonRuntime{}, snapshot: status},
	}
	response := &query.Response{}
	require.NoError(t, s.handleGetPythonUdfStatus(context.Background(), nil, response, nil))
	got := response.GetPythonUdfStatus
	require.Equal(t, "cn-local", got.CNUUID)
	require.Equal(t, udf.LanguagePython, got.Language)
	require.True(t, got.Enabled)
	require.True(t, got.AllowUnisolated)
	require.True(t, got.Ready)
	require.Equal(t, int32(1), got.ProtocolVersion)
	require.Equal(t, udf.PythonABIContract, got.ABIContract)
	require.Equal(t, uint64(42), got.LeaseEpoch)
	require.Equal(t, []string{"SCALAR", "VECTOR"}, got.Modes)
	require.Equal(t, []string{"CALLED_ON_NULL_INPUT", "RETURNS_NULL_ON_NULL_INPUT"}, got.NullPolicies)

	// The query service has a stable fail-closed status if an older/custom
	// Runtime implementation does not provide the bounded status contract.
	s.udfService = pythonRuntimeWithoutStatus{}
	response = &query.Response{}
	require.NoError(t, s.handleGetPythonUdfStatus(context.Background(), nil, response, nil))
	require.Equal(t, udf.RuntimeStatusUnavailable, response.GetPythonUdfStatus.ErrorClass)
	require.Equal(t, udf.RuntimeStatusReasonStatusUnavailable, response.GetPythonUdfStatus.Reason)
}
