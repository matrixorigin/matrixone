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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func stringNumericResultPipeline(t *testing.T) (*planpb.Query, *Scope) {
	t.Helper()
	args := []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	}
	expr, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "find_in_set", args)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int32), expr.Typ.Id)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{expr}
	return qry, &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func strictStringNumericCompatibilityPipeline(t *testing.T, sourceType types.T, overload int64) (*planpb.Query, *Scope) {
	t.Helper()
	stringValue := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(sourceType)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	// Use each serialized CAST identity that can reach the mode-aware
	// string-to-FLOAT executor; the protocol fence must not depend on overload
	// selection.
	cast := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: overload, ObjName: "cast"},
			Args: []*planpb.Expr{stringValue},
		}},
	}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{cast}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{cast}
	return qry, &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestStringNumericResultPlacementAndDestinationProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry, scope := stringNumericResultPipeline(t)
	scope.Proc = c.proc
	t.Cleanup(scope.RootOp.Release)

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion79
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a worker below the result-contract version must not receive a multi-CN plan")
	require.Equal(t, c.addr, c.cnList[0].Addr)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"a v80 worker keeps the multi-CN placement")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	client.version = defines.MORPCVersion79
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination",
		"a destination downgrade after placement must still be rejected")

	// An unaddressable worker is an unknown capability and must fail closed to
	// one-CN placement rather than assuming the current protocol is sufficient.
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
	require.Equal(t, c.addr, c.cnList[0].Addr)

	rt := runtime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion79)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc,
		&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
			ProjectList: []*planpb.Expr{qry.Nodes[0].ProjectList[0]},
		}}}), "version 80")
}

func TestStrictStringNumericCompatibilityMixedVersionFence(t *testing.T) {
	for _, sourceType := range []types.T{types.T_varchar, types.T_varbinary} {
		for _, overload := range []int64{0, 1, 2} {
			t.Run(fmt.Sprintf("source_%s_cast_overload_%d", sourceType.OidString(), overload), func(t *testing.T) {
				c, client := expressionProtocolTestCompile(t)
				qry, scope := strictStringNumericCompatibilityPipeline(t, sourceType, overload)
				scope.Proc = c.proc
				t.Cleanup(scope.RootOp.Release)

				features, err := planpb.RequiredRemoteExpressionFeatures(qry)
				require.NoError(t, err)
				require.True(t, features.StrictStringNumericCompatibility)
				require.False(t, features.NumericBinaryLiteralProvenance,
					"a plain string column cast does not carry mixed HEX/BIT row provenance")

				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				client.version = defines.MORPCVersion87
				require.NoError(t, c.constrainStrictStringNumericCompatibilityWorkers(qry))
				require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
					"new strict senders must not place changed string conversion semantics on v87 workers")

				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				client.version = defines.MORPCVersion94
				require.NoError(t, c.constrainStrictStringNumericCompatibilityWorkers(qry))
				require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
					"v94 workers can execute the strict contract remotely")

				client.version = defines.MORPCVersion87
				_, err = encodeRemoteScope(scope, c.proc)
				require.ErrorContains(t, err, "version 94",
					"a destination downgrade after placement must be rejected before sending")
				client.version = defines.MORPCVersion94
				data, err := encodeRemoteScope(scope, c.proc)
				require.NoError(t, err)
				require.NotEmpty(t, data)

				rt := runtime.ServiceRuntime(c.proc.GetService())
				oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
				t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
				rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion87)
				wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{qry.Nodes[0].ProjectList[0]}}}}
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "version 94")

				c.proc.GetSessionInfo().MySQLNumericCompatibilityMode = true
				require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline),
					"plain string conversion semantics remain compatible in explicit MySQL mode")
				c.proc.GetSessionInfo().MySQLNumericCompatibilityMode = false
				c.proc.GetSessionInfo().LegacyNumericCompatibilityMode = true
				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				client.version = defines.MORPCVersion94
				require.ErrorContains(t, c.constrainStrictStringNumericCompatibilityWorkers(qry), "legacy session contract",
					"placement must fail closed for a legacy sender as well")
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "legacy session contract",
					"a legacy sender marker must fail closed rather than enable permissive parsing")
			})
		}
	}
}

func TestNumericBinaryLiteralProvenanceAdmissionEveryModeAndConsumer(t *testing.T) {
	type mode struct {
		name   string
		mysql  bool
		native bool
	}
	modes := []mode{
		{name: "default"},
		{name: "mysql", mysql: true},
		{name: "native", native: true},
	}

	for _, producer := range []string{"case", "if", "coalesce"} {
		for _, consumer := range []string{"integer_cast", "string_condition"} {
			for _, mode := range modes {
				t.Run(producer+"/"+consumer+"/"+mode.name, func(t *testing.T) {
					c, client := expressionProtocolTestCompile(t)
					qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
					scope.Proc = c.proc
					t.Cleanup(scope.RootOp.Release)

					boolCondition := &planpb.Expr{
						Typ:  planpb.Type{Id: int32(types.T_bool)},
						Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
					}
					hex := &planpb.Expr{
						Typ: planpb.Type{Id: int32(types.T_varchar)},
						Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
							Value: &planpb.Literal_Sval{Sval: "1"},
							IsBin: true, LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_HEX,
						}},
					}
					text := &planpb.Expr{
						Typ:  planpb.Type{Id: int32(types.T_varchar)},
						Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1"}}},
					}
					var mixed *planpb.Expr
					switch producer {
					case "case":
						mixed = &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_varchar)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: int64(71) << 32, ObjName: "case"},
								Args: []*planpb.Expr{boolCondition, hex, text},
							}},
						}
					case "if":
						mixed = &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_varchar)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: int64(113) << 32, ObjName: "if"},
								Args: []*planpb.Expr{boolCondition, hex, text},
							}},
						}
					case "coalesce":
						nullableTextColumn := &planpb.Expr{
							Typ:  planpb.Type{Id: int32(types.T_varchar)},
							Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}},
						}
						mixed = &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_varchar)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: int64(74) << 32, ObjName: "coalesce"},
								Args: []*planpb.Expr{nullableTextColumn, hex},
							}},
						}
					}
					var expr *planpb.Expr
					switch consumer {
					case "integer_cast":
						expr = &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_int64)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: int64(21) << 32, ObjName: "cast"},
								Args: []*planpb.Expr{mixed},
							}},
						}
					case "string_condition":
						expr = &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_int64)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: int64(113) << 32, ObjName: "if"},
								Args: []*planpb.Expr{
									mixed,
									{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
									{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 2}}}},
								},
							}},
						}
					}
					qry.Nodes[0].ProjectList = []*planpb.Expr{expr}
					scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{expr}
					wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
					features, err := planpb.RequiredRemoteExpressionFeatures(qry)
					require.NoError(t, err)
					require.True(t, features.NumericBinaryLiteralProvenance,
						"the branch marker must be fenced independently of FLOAT casts and SQL mode")

					info := c.proc.GetSessionInfo()
					info.MySQLNumericCompatibilityMode = mode.mysql
					info.MatrixOneNativeMode = mode.native
					info.LegacyNumericCompatibilityMode = false
					rt := runtime.ServiceRuntime(c.proc.GetService())
					oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
					t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
					for _, version := range []int64{defines.MORPCVersion93, defines.MORPCVersion94} {
						client.version = version
						rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
						c.execType = plan.ExecTypeAP_MULTICN
						c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
						require.NoError(t, c.constrainStrictStringNumericCompatibilityWorkers(qry))
						if version < defines.MORPCVersion94 {
							require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
								"v93 placement must fall back locally in %s mode", mode.name)
						} else {
							require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
								"v94 placement should retain distributed execution in %s mode", mode.name)
						}

						protocolErr := validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline)
						if version < defines.MORPCVersion94 {
							require.ErrorContains(t, protocolErr, "version 94",
								"v93 receiver must reject the changed row-marker contract in %s mode", mode.name)
							_, err = encodeRemoteScope(scope, c.proc)
							require.ErrorContains(t, err, "version 94",
								"send-time destination downgrade must fail in %s mode", mode.name)
						} else {
							require.NoError(t, protocolErr)
							data, err := encodeRemoteScope(scope, c.proc)
							require.NoError(t, err)
							require.NotEmpty(t, data)
						}
					}

					client.version = defines.MORPCVersion94
					rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion94)
					info.LegacyNumericCompatibilityMode = true
					c.execType = plan.ExecTypeAP_MULTICN
					c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
					require.ErrorContains(t, c.constrainStrictStringNumericCompatibilityWorkers(qry), "legacy session contract")
					require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline), "legacy session contract")
				})
			}
		}
	}
}

func TestUniformNumericBinaryLiteralFlowDoesNotRequireV94(t *testing.T) {
	for _, mode := range []struct {
		name   string
		mysql  bool
		native bool
	}{
		{name: "default"},
		{name: "mysql", mysql: true},
		{name: "native", native: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
			scope.Proc = c.proc
			t.Cleanup(scope.RootOp.Release)

			condition := &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
			}
			literal := func(value string, form planpb.StringLiteralForm) *planpb.Expr {
				return &planpb.Expr{
					Typ: planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
						Value: &planpb.Literal_Sval{Sval: value},
						IsBin: true, LiteralForm: form,
					}},
				}
			}
			uniform := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: int64(71) << 32, ObjName: "case"},
					Args: []*planpb.Expr{
						condition,
						literal("1", planpb.StringLiteralForm_STRING_LITERAL_HEX),
						literal("001", planpb.StringLiteralForm_STRING_LITERAL_BIT),
					},
				}},
			}
			qry.Nodes[0].ProjectList = []*planpb.Expr{uniform}
			scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{uniform}
			features, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.False(t, features.NumericBinaryLiteralProvenance)

			info := c.proc.GetSessionInfo()
			info.MySQLNumericCompatibilityMode = mode.mysql
			info.MatrixOneNativeMode = mode.native
			wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{uniform}}}}
			rt := runtime.ServiceRuntime(c.proc.GetService())
			oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
			t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
			for _, version := range []int64{defines.MORPCVersion93, defines.MORPCVersion94} {
				client.version = version
				rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainStrictStringNumericCompatibilityWorkers(qry))
				require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
					"uniform scalar provenance does not require the v94 mixed-row trailer")
				require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline))
				data, err := encodeRemoteScope(scope, c.proc)
				require.NoError(t, err)
				require.NotEmpty(t, data)
			}
		})
	}
}

func TestStringNumericCompatibilityAdmissionByExpressionAndMode(t *testing.T) {
	for _, expression := range []struct {
		name       string
		historical bool
		id         int64
	}{
		{name: "cast"},
		{name: "if"},
		{name: "iff"},
		{name: "ceil", historical: true, id: 72},
		{name: "floor", historical: true, id: 103},
		{name: "float_int64"},
		{name: "ceil_scalar"},
	} {
		for _, mode := range []struct {
			name   string
			mysql  bool
			native bool
			value  string
		}{
			{name: "default", value: "1.5tail"},
			{name: "mysql", mysql: true, value: "1.5tail"},
			{name: "native", mysql: true, native: true, value: " 1.5 "},
		} {
			t.Run(expression.name+"/"+mode.name, func(t *testing.T) {
				c, client := expressionProtocolTestCompile(t)
				qry, scope := strictStringNumericCompatibilityPipeline(t, types.T_varchar, 0)
				scope.Proc = c.proc
				t.Cleanup(scope.RootOp.Release)
				expr := qry.Nodes[0].ProjectList[0]
				if expression.name == "ceil_scalar" {
					bound, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "ceil", []*planpb.Expr{
						{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
						{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "2"}}}},
					})
					require.NoError(t, err)
					require.NotNil(t, bound.GetF().Args[1].GetF())
					expr = bound
				}
				if expression.name == "float_int64" {
					// Bind a real precision column, not an explicit CAST1, so the
					// test proves ROUND's reachable ordinary FLOAT -> INT64 path.
					bound, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "round", []*planpb.Expr{
						{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 12.345}}}},
						{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					})
					require.NoError(t, err)
					expr = bound
				}
				if expression.name == "if" || expression.name == "iff" {
					condition := expr.GetF().Args[0]
					bound, err := plan.BindFuncExprImplByPlanExpr(context.Background(), expression.name, []*planpb.Expr{
						condition,
						{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 10}}}},
						{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 20}}}},
					})
					require.NoError(t, err)
					require.NotNil(t, bound.GetF())
					require.NotNil(t, bound.GetF().Args[0].GetCol())
					require.Equal(t, condition.Typ.Id, bound.GetF().Args[0].Typ.Id)
					require.Nil(t, bound.GetF().Args[0].GetF(), "binding must preserve the string condition without a CAST")
					expr = bound
				}
				if expression.historical {
					generated := &planpb.GeneratedCol{Expr: &planpb.Expr{
						Typ: planpb.Type{Id: int32(types.T_float64)},
						Expr: &planpb.Expr_F{F: &planpb.Function{
							Func: &planpb.ObjectRef{Obj: expression.id<<32 | 12, ObjName: expression.name},
							Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{
								Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: mode.value}},
							}}},
						}},
					}}
					wire, err := generated.MarshalBinary()
					require.NoError(t, err)
					decoded := &planpb.GeneratedCol{}
					require.NoError(t, decoded.UnmarshalBinary(wire))
					expr = decoded.Expr
				}
				qry.Nodes[0].ProjectList = []*planpb.Expr{expr}
				scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{expr}
				features, err := planpb.RequiredRemoteExpressionFeatures(qry)
				require.NoError(t, err)
				require.Equal(t, expression.name != "float_int64" && expression.name != "ceil_scalar", features.StrictStringNumericCompatibility)
				require.False(t, features.NumericBinaryLiteralProvenance,
					"ordinary string numeric expressions do not require the mixed-literal row trailer")
				require.Equal(t, expression.name == "float_int64", features.OrdinaryFloatInt64Bounds)
				require.Equal(t, expression.name == "ceil_scalar", features.ScalarMathPrecisionCompatibility)
				require.Equal(t, expression.historical, features.HistoricalStringMathCompatibility)
				info := c.proc.GetSessionInfo()
				info.MySQLNumericCompatibilityMode = mode.mysql
				info.MatrixOneNativeMode = mode.native
				info.LegacyNumericCompatibilityMode = false
				rt := runtime.ServiceRuntime(c.proc.GetService())
				oldVersion, _ := rt.GetGlobalVariables(runtime.MOProtocolVersion)
				t.Cleanup(func() { rt.SetGlobalVariables(runtime.MOProtocolVersion, oldVersion) })
				wirePipeline := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
				requiresCurrent := expression.historical || expression.name == "float_int64" ||
					expression.name == "ceil_scalar" || (!mode.mysql && !mode.native)
				for _, version := range []int64{defines.MORPCVersion94, defines.MORPCVersion93} {
					client.version = version
					c.execType = plan.ExecTypeAP_MULTICN
					c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
					require.NoError(t, c.constrainStrictStringNumericCompatibilityWorkers(qry))
					denied := requiresCurrent && version < defines.MORPCVersion94
					if denied {
						require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
						require.Equal(t, c.addr, c.cnList[0].Addr)
					} else {
						require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType)
					}
					data, err := encodeRemoteScope(scope, c.proc)
					if denied {
						require.ErrorContains(t, err, "version 94")
					} else {
						require.NoError(t, err)
						require.NotEmpty(t, data)
					}
					rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
					err = validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline)
					if denied {
						require.ErrorContains(t, err, "version 94")
					} else {
						require.NoError(t, err)
					}
				}
				client.version = defines.MORPCVersion94
				rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion94)
				info.LegacyNumericCompatibilityMode = true
				c.execType = plan.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				placementErr := c.constrainStrictStringNumericCompatibilityWorkers(qry)
				_, sendErr := encodeRemoteScope(scope, c.proc)
				receiveErr := validateRemoteExpressionPipelineProtocol(c.proc, wirePipeline)
				for _, err := range []error{placementErr, sendErr, receiveErr} {
					if requiresCurrent {
						require.ErrorContains(t, err, "legacy session contract")
					} else {
						require.NoError(t, err, "CAST and IF retain their explicit compatible legacy mappings")
					}
				}
			})
		}
	}
}
