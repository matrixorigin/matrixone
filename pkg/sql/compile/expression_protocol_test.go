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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
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
	onSend          func()
}

func (c *expressionVersionClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *expressionVersionClient) SendMessage(ctx context.Context, addr string, _ *query.Request) (*query.Response, error) {
	c.calls++
	if c.onSend != nil {
		defer c.onSend()
	}
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
func expressionProtocolTestCompile(t testing.TB) (*Compile, *expressionVersionClient) {
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

// Exercise the real send boundary, including serialization and all fences;
// version probes are counted independently by the query-client fixture.
func BenchmarkRemoteExpressionSend(b *testing.B) {
	for _, shape := range []string{"plain", "mixed", "wide"} {
		b.Run(shape, func(b *testing.B) {
			c, client := expressionProtocolTestCompile(b)
			client.version = defines.MORPCLatestVersion
			integer := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.PLUS, 2)},
				Args: []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
				},
			}}}
			ip, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "inet_aton", []*planpb.Expr{{
				Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}},
			}})
			require.NoError(b, err)
			expressions := []*planpb.Expr{integer, decimalDivisionProtocolExpr(types.T_decimal128), ip}
			if shape == "plain" {
				expressions = []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
			} else if shape == "wide" {
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
			}
			op := projection.NewArgument()
			op.ProjectList = expressions
			b.Cleanup(op.Release)
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := encodeRemoteScope(scope, c.proc); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(client.calls)/float64(b.N), "probes/op")
		})
	}
}

// A plan that needs rebinding is rejected before placement, whatever the
// execution type, and leaves the placement unchanged.
func TestRemoteExpressionPlacementRebindErrors(t *testing.T) {
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

	qry.Nodes[0].ProjectList = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.Zero(t, client.calls, "placement never probes peers")
}

func TestPythonRoutineProtocolChecksActualRemoteDestination(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{RoutineCall: &planpb.RoutineCall{}}}}
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.PythonRoutineContract)
	require.Equal(t, defines.MORPCVersion111, remoteExpressionProtocolVersion(features))
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion110, defines.MORPCVersion111} {
		client.version = version
		err := validateRemoteExpressionDestination(c.proc, p, features)
		if version < defines.MORPCVersion111 {
			require.ErrorContains(t, err, "remote destination does not support Python")
		} else {
			require.NoError(t, err)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion110)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "Python routine execution requires")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	require.Equal(t, client.calls, client.releases)
}
