// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package queryservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestProtocolMemoryCapabilityCompatibility(t *testing.T) {
	// A 4.1 peer only serializes field 1. Absence must disable the entire
	// distributed admission protocol before any new control stream is opened.
	var oldPeer query.GetProtocolVersionResponse
	require.NoError(t, oldPeer.Unmarshal([]byte{8, 42}))
	require.Equal(t, int64(42), oldPeer.Version)
	require.False(t, oldPeer.MemoryAdmission)
	require.Empty(t, oldPeer.MemoryAdmissionProcessId)
	require.Zero(t, oldPeer.MemoryAdmissionVersion)
	// The first admission implementation advertised only the bool and process
	// identity. It cannot account for the new producer descriptors.
	var boolOnlyPeer query.GetProtocolVersionResponse
	require.NoError(t, boolOnlyPeer.Unmarshal([]byte{8, 42, 16, 1, 26, 1, 'p'}))
	require.True(t, boolOnlyPeer.MemoryAdmission)
	require.Equal(t, "p", boolOnlyPeer.MemoryAdmissionProcessId)
	require.Zero(t, boolOnlyPeer.MemoryAdmissionVersion)
	service := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
	runtime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, int64(42))
	s := &queryService{serviceID: service}
	response := &query.Response{}
	require.NoError(t, s.handleGetProtocolVersion()(context.Background(), &query.Request{GetProtocolVersion: &query.GetProtocolVersionRequest{}}, response, nil))
	require.NoError(t, response.UnwrapError())
	require.True(t, response.GetProtocolVersion.MemoryAdmission)
	require.Equal(t, runtime.ProcessInstanceID(), response.GetProtocolVersion.MemoryAdmissionProcessId)
	require.Equal(t, query.MemoryAdmissionProtocolVersion, response.GetProtocolVersion.MemoryAdmissionVersion)
	wire, err := response.GetProtocolVersion.Marshal()
	require.NoError(t, err)
	require.Equal(t, []byte{8, 42, 16, 1}, wire[:4], "existing field number and version are preserved")
	var current query.GetProtocolVersionResponse
	require.NoError(t, current.Unmarshal(wire))
	require.True(t, current.MemoryAdmission)
	require.Equal(t, runtime.ProcessInstanceID(), current.MemoryAdmissionProcessId)
	require.Equal(t, query.MemoryAdmissionProtocolVersion, current.MemoryAdmissionVersion)
}
