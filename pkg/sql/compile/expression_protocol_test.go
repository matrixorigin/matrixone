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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type expressionVersionClient struct {
	fakeQueryClient
	version         int64
	versions        map[string]int64
	calls, releases int
	customResponse  bool
	response        *query.Response
	sendErr         error
}

func (c *expressionVersionClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *expressionVersionClient) SendMessage(ctx context.Context, addr string, _ *query.Request) (*query.Response, error) {
	c.calls++
	if c.customResponse {
		return c.response, c.sendErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	version := c.version
	if c.versions != nil {
		version = c.versions[addr]
	}
	return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{Version: version}}, nil
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

func TestRemoteExpressionPlacementRechecksGenerationAndWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	ip, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "inet_aton", []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}})
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
		ip, decimalDivisionProtocolExpr(types.T_decimal128),
	}}}, Steps: []int32{0}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	cluster, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster.(*schedulerTestCluster).cns = append(cluster.(*schedulerTestCluster).cns,
		metadata.CNService{ServiceID: "second-worker", QueryAddress: "second:9000", PipelineServiceAddress: "second:6001"})
	workers := engine.Nodes{
		{Id: "old-worker", Addr: "remote:6001", Mcpu: 4},
		{Id: "second-worker", Addr: "second:6001", Mcpu: 4},
	}
	client.versions = map[string]int64{"worker:9000": defines.MORPCVersion97, "second:9000": defines.MORPCVersion96}
	place := func(want plan2.ExecType, probes int) {
		t.Helper()
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = workers
		calls, releases := client.calls, client.releases
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
		require.Equal(t, probes, client.calls-calls)
		require.Equal(t, probes, client.releases-releases)
		if want == plan2.ExecTypeAP_ONECN {
			require.Len(t, c.cnList, 1)
			require.Equal(t, c.addr, c.cnList[0].Addr)
		}
	}
	// The lower-version feature succeeds on both workers, but DIV still
	// requires v97 on the second. One probe per worker proves both floors.
	place(plan2.ExecTypeAP_ONECN, 2)
	client.versions["second:9000"] = defines.MORPCVersion97
	place(plan2.ExecTypeAP_MULTICN, 2)
	// Mutating the same query and reusing Compile must recompute its floor.
	qry.Nodes[0].ProjectList = []*planpb.Expr{ip}
	client.versions["second:9000"] = defines.MORPCVersion72
	place(plan2.ExecTypeAP_MULTICN, 2)
	client.versions["second:9000"] = defines.MORPCVersion71
	place(plan2.ExecTypeAP_ONECN, 2)
	qry.Nodes[0].ProjectList = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}
	place(plan2.ExecTypeAP_MULTICN, 0)
}

func TestRemoteExpressionPlacementLocalRebindAndErrors(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	legacy := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL, 0)},
	}}}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{legacy}}}}
	for _, kind := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
		c.execType = kind
		require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "legacy interval")
		require.Equal(t, kind, c.execType)
	}
	malformed := integerProtocolExpr(function.IntegerArgumentCastOverload)
	malformed.GetF().Args = nil
	qry.Nodes[0].ProjectList = []*planpb.Expr{malformed}
	require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "CAST arity")
	require.Zero(t, client.calls)
	qry.Nodes[0].ProjectList = []*planpb.Expr{decimalDivisionProtocolExpr(types.T_decimal128)}
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, c.constrainRemoteExpressionWorkers(qry), context.Canceled)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.Zero(t, client.calls)
}
