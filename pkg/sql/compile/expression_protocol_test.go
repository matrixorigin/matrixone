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

	"github.com/gogo/protobuf/proto"
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	c.proc.SetStmtProfile(&process.StmtProfile{})
	c.proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
	c.proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
	c.proc.SetResolveVariableFunc(nil)
	require.True(t, c.proc.DivisionByZeroWarningsEnabled())
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{RoutineCall: &planpb.RoutineCall{}}}}
	loadFK, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "_load_fk_ignore_check", []*planpb.Expr{
		{Typ: types.T_bool.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 6}}},
	})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(602, 0), loadFK.GetF().Func.Obj)
	viewDefinition, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "mo_view_definition", []*planpb.Expr{
		{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 7}}},
	})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(603, 0), viewDefinition.GetF().Func.Obj)
	calendar := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_CALENDAR_INTERVAL, 0)}}}}
	division, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	stringType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	stringType.CollationVersion = 1
	least, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "least", []*planpb.Expr{
		{Typ: stringType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		{Typ: stringType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}},
	})
	require.NoError(t, err)
	tableCollation, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "internal_table_collation", []*planpb.Expr{
		{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: ""}}}},
	})
	require.NoError(t, err)
	decimalDiv := decimalDivisionProtocolExpr(types.T_uint64)
	decimalDiv.Typ = planpb.Type{Id: int32(types.T_uint64), Width: 7, Scale: 0}
	decimalDiv.GetF().Func.Obj = function.EncodeOverloadID(function.INTEGER_DIV, function.DecimalIntegerDivUnsignedOverload)
	timestamp, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "timestamp", []*planpb.Expr{{Typ: types.T_datetime.ToTypeWithScale(3).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}}}})
	require.NoError(t, err)
	dynamicTime := &planpb.Expr{Typ: types.T_time.ToTypeWithScale(6).PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.ADDTIME, 2)},
		Args: []*planpb.Expr{{Typ: types.T_datetime.ToTypeWithScale(6).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 4}}}, plan2.MakePlan2StringConstExprWithType("02:00:00.000001")},
	}}}
	fractionalSecond := &planpb.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TIMESTAMPADD_SECOND_INTERVAL, 5)},
		Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 7}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 5}}}},
	}}}
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr, calendar, division, least, tableCollation, decimalDiv, timestamp, dynamicTime, fractionalSecond, loadFK, viewDefinition}}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.PythonRoutineContract)
	require.True(t, features.LoadFKIgnoreCheck)
	require.True(t, features.ViewMetadataFunctions)
	require.True(t, features.TimestampDatetimeResultContracts)
	require.True(t, features.DynamicTimeResultContracts)
	require.True(t, features.TimestampAddSecondIntervals)
	require.True(t, features.DecimalIntegerDivisionSemantics)
	require.True(t, features.CalendarIntervalNormalization)
	require.True(t, features.ScalarDivision)
	require.True(t, features.LeastGreatestCollationSemantics)
	require.True(t, features.TableCollationFunction)
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
	require.Equal(t, defines.MORPCVersion130, executionRemoteExpressionProtocolVersion(c.proc, features))
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion126, defines.MORPCVersion130} {
		client.version = version
		err := validateRemoteExpressionDestination(c.proc, p, features)
		if version < defines.MORPCVersion130 {
			require.ErrorContains(t, err, "remote destination does not support Python")
		} else {
			require.NoError(t, err)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "Python routine execution requires")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))

	// The existing execution stream must negotiate the complete encoded contract.
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = p.InstructionList[0].ProjectList
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	var required int64
	wire, err := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, required)
	decoded, err := decodeScope(wire, c.proc, true, nil)
	require.NoError(t, err)
	decoded.release()
	// Python alone needs the same stream fence without a temporal feature.
	op.ProjectList = []*planpb.Expr{expr}
	required = 0
	_, err = encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, required)

	// Capability does not transfer the coordinator's dependent-program owner.
	dependent := &planpb.Expr{Typ: decimalDiv.Typ, Expr: &planpb.Expr_Dependent{Dependent: &planpb.DependentExpr{
		ProgramId: 1, Inputs: []*planpb.Expr{expr, decimalDiv},
	}}}
	op.ProjectList = []*planpb.Expr{dependent}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "coordinator execution owner")
	data, err := (&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{dependent}}}}).Marshal()
	require.NoError(t, err)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "coordinator execution owner")

	// Immediate-predecessor rollback disables Python but preserves fractional
	// TIMESTAMPADD at124, LOAD FK at125, VIEWS at126, temporal122 and DIV121.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	_, err = decodeScope(wire, c.proc, true, nil)
	require.ErrorContains(t, err, "Python routine execution requires")
	client.version = defines.MORPCVersion126
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "remote destination does not support Python")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "Python routine execution requires")
	// The predecessor retains the official VIEWS contract independently of Python.
	viewOnly := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{viewDefinition}}}}
	viewFeatures, err := planpb.RequiredRemoteExpressionFeatures(viewOnly)
	require.NoError(t, err)
	require.True(t, viewFeatures.Any())
	require.True(t, viewFeatures.ViewMetadataFunctions)
	require.False(t, viewFeatures.PythonRoutineContract)
	require.Equal(t, defines.MORPCVersion126, remoteExpressionProtocolVersion(viewFeatures))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, viewOnly, viewFeatures))
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, viewOnly))
	op.ProjectList = viewOnly.InstructionList[0].ProjectList
	required = 0
	viewWire, err := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion126, required)
	decoded, err = decodeScope(viewWire, c.proc, true, nil)
	require.NoError(t, err)
	decoded.release()
	client.version = defines.MORPCVersion125
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, viewOnly, viewFeatures), "version 126")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, viewOnly), "view metadata remote execution requires")
	_, err = decodeScope(viewWire, c.proc, true, nil)
	require.ErrorContains(t, err, "view metadata remote execution requires")
	loadOnly := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{loadFK}}}}
	loadFeatures, err := planpb.RequiredRemoteExpressionFeatures(loadOnly)
	require.NoError(t, err)
	require.False(t, loadFeatures.PythonRoutineContract)
	require.True(t, loadFeatures.LoadFKIgnoreCheck)
	require.Equal(t, defines.MORPCVersion125, remoteExpressionProtocolVersion(loadFeatures))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, loadOnly, loadFeatures))
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, loadOnly))
	op.ProjectList = loadOnly.InstructionList[0].ProjectList
	required = 0
	loadWire, err := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion125, required)
	decoded, err = decodeScope(loadWire, c.proc, true, nil)
	require.NoError(t, err)
	decoded.release()
	client.version = defines.MORPCVersion124
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion124)
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, loadOnly, loadFeatures), "version 125")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, loadOnly), "foreign-key filtering requires")
	_, err = decodeScope(loadWire, c.proc, true, nil)
	require.ErrorContains(t, err, "foreign-key filtering requires")
	fractionalOnly := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{fractionalSecond}}}}
	fractionalFeatures, err := planpb.RequiredRemoteExpressionFeatures(fractionalOnly)
	require.NoError(t, err)
	require.False(t, fractionalFeatures.PythonRoutineContract)
	require.True(t, fractionalFeatures.TimestampAddSecondIntervals)
	require.Equal(t, defines.MORPCVersion124, remoteExpressionProtocolVersion(fractionalFeatures))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, fractionalOnly, fractionalFeatures))
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, fractionalOnly))
	op.ProjectList = fractionalOnly.InstructionList[0].ProjectList
	required = 0
	fractionalWire, err := encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion124, required)
	decoded, err = decodeScope(fractionalWire, c.proc, true, nil)
	require.NoError(t, err)
	decoded.release()
	client.version = defines.MORPCVersion123
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion123)
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, fractionalOnly, fractionalFeatures), "version 124")
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, fractionalOnly), "fractional TIMESTAMPADD SECOND requires")
	_, err = decodeScope(fractionalWire, c.proc, true, nil)
	require.ErrorContains(t, err, "fractional TIMESTAMPADD SECOND requires")
	client.version = defines.MORPCVersion124
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion124)
	temporalOnly := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{timestamp, dynamicTime}}}}
	temporalFeatures, err := planpb.RequiredRemoteExpressionFeatures(temporalOnly)
	require.NoError(t, err)
	require.False(t, temporalFeatures.PythonRoutineContract)
	require.Equal(t, defines.MORPCVersion122, remoteExpressionProtocolVersion(temporalFeatures))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, temporalOnly, temporalFeatures))
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, temporalOnly))
	op.ProjectList = temporalOnly.InstructionList[0].ProjectList
	required = 0
	_, err = encodeRemoteScopeWithProtocol(scope, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion122, required)

	divOnly := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{decimalDiv}}}}
	divFeatures, err := planpb.RequiredRemoteExpressionFeatures(divOnly)
	require.NoError(t, err)
	require.False(t, divFeatures.PythonRoutineContract)
	require.True(t, divFeatures.DecimalIntegerDivisionSemantics)
	require.Equal(t, defines.MORPCVersion121, remoteExpressionProtocolVersion(divFeatures))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, divOnly, divFeatures))
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, divOnly))
	require.Equal(t, client.calls, client.releases)
}

func TestPythonRoutineProtocolInsideIndexSearchScan(t *testing.T) {
	for _, field := range []string{"query_payload", "algo_expr"} {
		t.Run(field, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			call := &planpb.RoutineCall{FunctionRef: &planpb.FunctionRef{FunctionId: 7, Revision: 3}, Language: "python"}
			expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{RoutineCall: call}}}
			scan := &planpb.IndexSearchScan{AlgoOptions: []byte(`{}`)}
			if field == "query_payload" {
				scan.QueryPayload = expr
			} else {
				scan.AlgoExprs = []*planpb.Expr{expr}
				scan.AlgoExprNames = []string{"first_round_limit"}
			}
			cloned := plan2.DeepCopyNode(&planpb.Node{NodeType: planpb.Node_INDEX_SEARCH_SCAN, IndexSearchScan: scan})
			require.True(t, proto.Equal(scan, cloned.IndexSearchScan))
			var clonedExpr *planpb.Expr
			if field == "query_payload" {
				clonedExpr = cloned.IndexSearchScan.QueryPayload
			} else {
				clonedExpr = cloned.IndexSearchScan.AlgoExprs[0]
			}
			require.NotSame(t, call, clonedExpr.GetF().RoutineCall)
			require.NotSame(t, call.FunctionRef, clonedExpr.GetF().RoutineCall.FunctionRef)
			p := &pipeline.Pipeline{
				Node:       &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
				DataSource: &pipeline.Source{Node: cloned},
			}
			data, err := p.Marshal()
			require.NoError(t, err)
			var received pipeline.Pipeline
			require.NoError(t, received.Unmarshal(data))
			require.True(t, proto.Equal(scan, received.DataSource.Node.IndexSearchScan))
			features, err := planpb.RequiredRemoteExpressionFeatures(&received)
			require.NoError(t, err)
			require.True(t, features.PythonRoutineContract)
			require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			client.version = defines.MORPCVersion126
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, &received, features), "remote destination does not support Python")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &received), "Python routine execution requires")
			client.version = defines.MORPCVersion130
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
			require.NoError(t, validateRemoteExpressionDestination(c.proc, &received, features))
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &received))
			require.Equal(t, client.calls, client.releases)
		})
	}
}
