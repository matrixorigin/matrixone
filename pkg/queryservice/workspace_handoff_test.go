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

package queryservice

import (
	"context"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceHandoffCapabilityFollowsEngineLifecycle(t *testing.T) {
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	rt := moruntime.ServiceRuntime(sid)
	svc := &queryService{serviceID: sid}
	call := svc.handleGetProtocolVersion()
	req := &query.Request{GetProtocolVersion: &query.GetProtocolVersionRequest{}}
	resp := &query.Response{}
	require.NoError(t, call(context.Background(), req, resp, nil))
	require.Zero(t, resp.GetProtocolVersion.WorkspaceHandoffVersion)

	handler := &moruntime.WorkspaceHandoffHandler{Handle: func(data []byte) ([]byte, error) {
		return append([]byte(nil), data...), nil
	}}
	rt.SetGlobalVariables(moruntime.WorkspaceHandoffHandlerKey, handler)
	req.GetProtocolVersion.WorkspaceHandoffRequest = []byte("request")
	resp = &query.Response{}
	require.NoError(t, call(context.Background(), req, resp, nil))
	require.EqualValues(t, 1, resp.GetProtocolVersion.WorkspaceHandoffVersion)
	require.Equal(t, []byte("request"), resp.GetProtocolVersion.WorkspaceHandoffResponse)
	require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.WorkspaceHandoffHandlerKey, handler))
	resp = &query.Response{}
	require.Error(t, call(context.Background(), req, resp, nil))
}

func TestWorkspaceHandoffCapabilityOverQueryRPC(t *testing.T) {
	cn := metadata.CNService{ServiceID: "handoff-query-rpc"}
	runTestWithQueryService(t, cn, nil, func(client queryclient.QueryClient, address string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req := client.NewRequest(query.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &query.GetProtocolVersionRequest{}
		resp, err := client.SendMessage(ctx, address, req)
		require.NoError(t, err)
		require.Zero(t, resp.GetProtocolVersion.WorkspaceHandoffVersion)
		client.Release(resp)

		rt := moruntime.ServiceRuntime(cn.ServiceID)
		handler := &moruntime.WorkspaceHandoffHandler{Handle: func(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }}
		rt.SetGlobalVariables(moruntime.WorkspaceHandoffHandlerKey, handler)
		defer rt.CompareAndDeleteGlobalVariables(moruntime.WorkspaceHandoffHandlerKey, handler)
		req = client.NewRequest(query.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &query.GetProtocolVersionRequest{WorkspaceHandoffRequest: []byte("receipt")}
		resp, err = client.SendMessage(ctx, address, req)
		require.NoError(t, err)
		require.EqualValues(t, 1, resp.GetProtocolVersion.WorkspaceHandoffVersion)
		require.Equal(t, []byte("receipt"), resp.GetProtocolVersion.WorkspaceHandoffResponse)
		client.Release(resp)
	})
}
