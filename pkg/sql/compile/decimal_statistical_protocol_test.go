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
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestDecimalStatisticalProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "var_pop", []*planpb.Expr{{Typ: types.New(types.T_decimal128, 38, 18).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.DecimalStatisticalResultContracts)
	require.Equal(t, defines.MORPCVersion113, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{AggList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64, want plan2.ExecType) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
	}
	place(defines.MORPCVersion112, plan2.ExecTypeAP_ONECN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 113")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	place(defines.MORPCVersion113, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion112
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 113", "send checks changed actual destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 113")
	require.Equal(t, client.calls, client.releases)
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion113} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		validationErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		if version == defines.MORPCVersion112 {
			require.ErrorContains(t, validationErr, "version 113")
			require.ErrorContains(t, decodeErr, "version 113")
		} else {
			require.NoError(t, validationErr)
			require.NoError(t, decodeErr)
		}
	}
	// A modern receiver must not reinterpret the old DECIMAL result vector ABI.
	expr.Typ = types.New(types.T_decimal128, 38, 18).PlanType()
	data, err = p.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "rebinding")
	require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "rebinding")
}

func TestDecimalStatisticalAggregateDescriptorProtocol(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, id := range []int64{aggexec.AggIdOfVarPop, aggexec.AggIdOfVarSample, aggexec.AggIdOfStdDevPop, aggexec.AggIdOfStdDevSample} {
		for _, distinct := range []bool{false, true} {
			for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float64, types.T_int64} {
				args := []*planpb.Expr{{Typ: oid.ToType().PlanType()}}
				aggs := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(id, distinct, args, nil)}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion112)
				err := validateRemoteAggregateProtocol(c.proc, aggs)
				if oid == types.T_decimal64 || oid == types.T_decimal128 {
					require.ErrorContains(t, err, "version 113")
					require.ErrorContains(t, validateRemoteAggregateProtocol(nil, aggs), "version 113")
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, "113")
					require.ErrorContains(t, validateRemoteAggregateProtocol(c.proc, aggs), "version 113")
				} else {
					require.NoError(t, err)
				}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
				require.NoError(t, validateRemoteAggregateProtocol(c.proc, aggs))
			}
		}
	}
	// The result contract does not replace the historical variance state floor.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	require.ErrorContains(t, validateRemoteAggregateProtocol(c.proc, []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(function.EncodeOverloadID(function.VAR_POP, 0), false, []*planpb.Expr{{Typ: types.T_float64.ToType().PlanType()}}, nil)}), "version 35")
}

// Exercise admission through the receiver before any pooled operator is built.
func TestDecimalStatisticalDescriptorReceive(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	double := types.T_float64.ToType().PlanType()
	decimal := types.New(types.T_decimal128, 38, 18).PlanType()
	integer := types.T_int64.ToType().PlanType()
	for _, op := range []int32{int32(vm.Group), int32(vm.MergeGroup)} {
		for _, nested := range []bool{false, true} {
			for _, id := range []int64{aggexec.AggIdOfVarPop, aggexec.AggIdOfVarSample, aggexec.AggIdOfStdDevPop, aggexec.AggIdOfStdDevSample} {
				for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float64, types.T_int64} {
					t.Run(fmt.Sprintf("op%d/nested%t/agg%d/%s", op, nested, id, oid), func(t *testing.T) {
						affected := oid == types.T_decimal64 || oid == types.T_decimal128
						wire := &pipeline.Aggregate{Op: id, Dist: true, Expr: []*planpb.Expr{{Typ: oid.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}}
						leaf := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: op, Agg: &pipeline.Group{NeedEval: true, Aggs: []*pipeline.Aggregate{wire}}}}}
						p := leaf
						if nested {
							p = &pipeline.Pipeline{Children: []*pipeline.Pipeline{leaf}}
						}
						if affected {
							wire.ResultType = &double
						}
						data, err := p.Marshal()
						require.NoError(t, err)
						for _, version := range []any{nil, "113", defines.MORPCVersion112, defines.MORPCVersion113} {
							rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
							if !affected && (version == nil || version == "113") {
								continue // historical variance-state gate requires a known floor
							}
							scope, decodeErr := decodeScope(data, c.proc, true, nil)
							if scope != nil {
								t.Cleanup(scope.release)
							}
							if affected && version != defines.MORPCVersion113 {
								require.ErrorContains(t, decodeErr, "version 113")
								require.Nil(t, scope)
							} else {
								require.NoError(t, decodeErr)
								require.NotNil(t, scope)
							}
						}
						if !affected {
							return
						}
						rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
						for _, contract := range []*planpb.Type{nil, &decimal, &integer} {
							wire.ResultType = contract
							data, err = p.Marshal()
							require.NoError(t, err)
							scope, decodeErr := decodeScope(data, c.proc, true, nil)
							require.ErrorContains(t, decodeErr, "rebinding", "legacy or incompatible result ABI")
							require.Nil(t, scope, "reject before constructing operator owners")
						}
					})
				}
			}
		}
	}
}

func TestDecimalStatisticalDescriptorSendDestination(t *testing.T) {
	for _, merge := range []bool{false, true} {
		for _, nested := range []bool{false, true} {
			for _, name := range []string{"var_pop", "var_samp", "stddev_pop", "stddev_samp"} {
				for _, oid := range []types.T{types.T_decimal64, types.T_decimal128} {
					t.Run(fmt.Sprintf("merge%t/nested%t/%s/%s", merge, nested, name, oid), func(t *testing.T) {
						c, client := expressionProtocolTestCompile(t)
						expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, []*planpb.Expr{{Typ: types.New(oid, 18, 18).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
						require.NoError(t, err)
						qry := &planpb.Query{Nodes: []*planpb.Node{{AggList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
						client.version = defines.MORPCVersion113
						c.execType = plan2.ExecTypeAP_MULTICN
						c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
						require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
						require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
						// Native lowering retains only the ID and argument expressions.
						aggs := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(expr.GetF().Func.Obj, false, expr.GetF().Args, nil)}
						var op vm.Operator
						if merge {
							g := group.NewArgumentMergeGroup()
							t.Cleanup(g.Release)
							g.Aggs = aggs
							op = g
						} else {
							g := group.NewArgument()
							t.Cleanup(g.Release)
							g.NeedEval, g.Aggs = true, aggs
							op = g
						}
						scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: c.cnList[0], RootOp: op}
						if nested {
							scope = &Scope{Magic: Remote, Proc: c.proc, NodeInfo: c.cnList[0], PreScopes: []*Scope{scope}}
						}
						for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion112} {
							client.version = version
							before := client.calls
							data, sendErr := encodeRemoteScope(scope, c.proc)
							require.Equal(t, before+1, client.calls, "one fresh actual-destination probe per send")
							if version == defines.MORPCVersion112 {
								require.ErrorContains(t, sendErr, "version 113")
								require.Nil(t, data)
							} else {
								require.NoError(t, sendErr)
								decoded, decodeErr := decodeScope(data, c.proc, true, nil)
								require.NoError(t, decodeErr)
								require.NotNil(t, decoded)
								t.Cleanup(decoded.release)
								p := &pipeline.Pipeline{}
								require.NoError(t, p.Unmarshal(data))
								if nested {
									p = p.Children[0]
								}
								require.Equal(t, int32(types.T_float64), p.InstructionList[0].Agg.Aggs[0].GetResultType().GetId())
							}
						}
						// Native and expression requirements must share the same observation.
						switch g := op.(type) {
						case *group.Group:
							g.ProjectList = []*planpb.Expr{expr}
						case *group.MergeGroup:
							g.ProjectList = []*planpb.Expr{expr}
						}
						client.version = defines.MORPCVersion113
						before := client.calls
						_, err = encodeRemoteScope(scope, c.proc)
						require.NoError(t, err)
						require.Equal(t, before+1, client.calls)
						client.customResponse, client.response = true, &query.Response{}
						_, err = encodeRemoteScope(scope, c.proc)
						require.ErrorContains(t, err, "version 113", "unknown destination fails closed")
						require.Equal(t, client.calls, client.releases)
					})
				}
			}
		}
	}
}
