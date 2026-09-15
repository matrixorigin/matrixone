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

package compile

import (
	"context"
	"errors"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type expressionVersionClient struct {
	fakeQueryClient
	version         int64
	calls, releases int
	customResponse  bool
	response        *query.Response
	sendErr         error
}

func (c *expressionVersionClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *expressionVersionClient) SendMessage(ctx context.Context, _ string, _ *query.Request) (*query.Response, error) {
	c.calls++
	if c.customResponse {
		return c.response, c.sendErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{Version: c.version}}, nil
}
func (c *expressionVersionClient) Release(*query.Response) { c.releases++ }
func expressionProtocolTestCompile(t *testing.T) (*Compile, *expressionVersionClient) {
	t.Helper()
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 4
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldCluster, hadCluster := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster := &schedulerTestCluster{cns: []metadata.CNService{{ServiceID: "old-worker", QueryAddress: "worker:9000", PipelineServiceAddress: "remote:6001"}}}
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		if hadCluster {
			rt.SetGlobalVariables(moruntime.ClusterService, oldCluster)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ClusterService, cluster)
		}
	})
	client := &expressionVersionClient{version: defines.MORPCVersion66}
	c.proc.Base.QueryClient = client
	return c, client
}
func TestExpressionProtocolUnknownAndCanceledWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.Base.QueryClient = nil
	supported, err := remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	c.proc.Base.QueryClient = client
	client.version = defines.MORPCVersion67
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "stale:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported, "a new query endpoint cannot validate a stale pipeline address")
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "wrong-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	_, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion67)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, client.calls)
}

func TestExpressionProtocolResolvesLegacyAddressOnlyScopes(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion67
	supported, err := remoteWorkersSupportProtocol(c.proc,
		engine.Nodes{{Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, 1, client.calls)
	require.Equal(t, client.calls, client.releases)
}

func TestExpressionProtocolFailedResponsesAreReleased(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *query.Response
		err      error
	}{
		{"missing response", nil, nil},
		{"missing version", &query.Response{}, nil},
		{"error without response", nil, errors.New("probe failed")},
		{"error with response", &query.Response{}, errors.New("probe failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.customResponse = true
			client.response = tc.response
			client.sendErr = tc.err
			ok, err := remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
			require.NoError(t, err)
			require.False(t, ok, "unknown capabilities must select local placement")
			want := 0
			if tc.response != nil {
				want = 1
			}
			require.Equal(t, want, client.releases)
		})
	}
}
