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
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
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

func TestRemoteExpressionSendReusesProbeAndRechecksNextSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	integer := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.PLUS, 2)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{integer, decimalDivisionProtocolExpr(types.T_decimal128)}
	t.Cleanup(op.Release)
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, tc := range []struct {
		version int64
		message string
	}{
		{defines.MORPCVersion70, "checked integer arithmetic (MORPC version 71)"},
		{defines.MORPCVersion90, "decimal division (MORPC version 97)"},
		{defines.MORPCVersion97, ""},
	} {
		client.version = tc.version
		calls, releases := client.calls, client.releases
		data, err := encodeRemoteScope(scope, c.proc)
		if tc.message == "" {
			require.NoError(t, err)
			require.NotEmpty(t, data)
		} else {
			require.ErrorContains(t, err, tc.message)
		}
		require.Equal(t, 1, client.calls-calls)
		require.Equal(t, 1, client.releases-releases)
	}
	// Reuse the same Scope, changing its expression generation after successful
	// encoding: an old observation cannot authorize a later send.
	op.ProjectList = []*planpb.Expr{integer}
	client.version = defines.MORPCVersion71
	before := client.calls
	_, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, 1, client.calls-before)
	op.ProjectList = nil
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, 1, client.calls-before, "no expression floor means no probe")
}

func TestRemoteExpressionDestinationPreservesLiveGuards(t *testing.T) {
	for _, mode := range []string{"cancel", "downgrade", "upgrade"} {
		t.Run(mode, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			features := planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, RowDependentConvBases: true}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				defer cancel()
				c.proc.Ctx = ctx
				client.onSend = cancel
			} else if mode == "downgrade" {
				client.onSend = func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion69) }
			} else {
				features = planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, DecimalDivisionSemantics: true}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion71)
				client.onSend = func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97) }
			}
			err := validateRemoteExpressionDestination(c.proc, p, features)
			if mode == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			} else if mode == "downgrade" {
				require.ErrorContains(t, err, "row-dependent CONV bases (MORPC version 70)")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, client.calls)
			require.Equal(t, 1, client.releases)
		})
	}
}

func TestRemoteExpressionDestinationUnknownEvidence(t *testing.T) {
	for _, mode := range []string{"nil", "empty", "error", "error-response", "wrong-id", "stale-address", "no-client", "no-node", "no-features"} {
		t.Run(mode, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			f := planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, DecimalDivisionSemantics: true}
			expectedCalls, expectedReleases := 0, 0
			switch mode {
			case "nil", "empty", "error", "error-response":
				client.customResponse = true
				expectedCalls = 1
				if mode == "empty" || mode == "error-response" {
					client.response = &query.Response{}
					expectedReleases = 1
				}
				if mode == "error" || mode == "error-response" {
					client.sendErr = errors.New("probe failed")
				}
			case "wrong-id":
				p.Node.Id = "other-worker"
			case "stale-address":
				p.Node.Addr = "stale:6001"
			case "no-client":
				c.proc.Base.QueryClient = nil
			case "no-node":
				p.Node = nil
			case "no-features":
				f = planpb.RemoteExpressionFeatures{}
			}
			err := validateRemoteExpressionDestination(c.proc, p, f)
			if mode == "no-features" {
				require.NoError(t, err)
			} else if mode == "no-node" {
				require.ErrorContains(t, err, "versioned remote destination")
			} else {
				require.ErrorContains(t, err, "checked integer arithmetic (MORPC version 71)")
			}
			require.Equal(t, expectedCalls, client.calls)
			require.Equal(t, expectedReleases, client.releases)
		})
	}
}

func TestPythonRoutineProtocolChecksActualRemoteDestination(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{RoutineCall: &planpb.RoutineCall{}}}}
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.PythonRoutineContract)
	require.Equal(t, defines.MORPCVersion110, remoteExpressionProtocolVersion(features))
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion109, defines.MORPCVersion110} {
		client.version = version
		err := validateRemoteExpressionDestination(c.proc, p, features)
		if version < defines.MORPCVersion110 {
			require.ErrorContains(t, err, "remote destination does not support Python")
		} else {
			require.NoError(t, err)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion109)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "Python routine execution requires")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion110)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	require.Equal(t, client.calls, client.releases)
}
