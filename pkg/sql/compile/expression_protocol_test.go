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
}

func (c *expressionVersionClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *expressionVersionClient) SendMessage(ctx context.Context, _ string, _ *query.Request) (*query.Response, error) {
	c.calls++
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
	cluster := &schedulerTestCluster{cns: []metadata.CNService{{ServiceID: "old-worker", QueryAddress: "worker:9000"}}}
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
	client := &expressionVersionClient{version: defines.MORPCVersion64}
	c.proc.Base.QueryClient = client
	return c, client
}
func TestExpressionProtocolUnknownAndCanceledWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.Base.QueryClient = nil
	supported, err := remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion65)
	require.NoError(t, err)
	require.False(t, supported)
	c.proc.Base.QueryClient = client
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{}}, defines.MORPCVersion65)
	require.NoError(t, err)
	require.False(t, supported)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	_, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion65)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, client.calls)
}
