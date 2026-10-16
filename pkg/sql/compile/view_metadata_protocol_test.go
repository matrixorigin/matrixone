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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataSenderOrdinaryPayloadNeedsNoProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	// Scalar bytes are opaque data, not expressions or function identities.
	p := &pipeline.Pipeline{
		Node:       &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
		DataSource: &pipeline.Source{MembershipFilter: []byte{1}},
		InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{
			{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: []byte{1}}}},
			{Expr: &planpb.Expr_Fold{Fold: &planpb.FoldVal{Data: []byte{1}}}},
		}}},
	}
	// Main owns the previous VIEWS candidate IDs for hyperbolic and format functions.
	// Those functions must not acquire a VIEWS floor or capability probe.
	for _, tc := range []struct {
		id         int32
		resultType types.T
	}{
		{function.TANH, types.T_float64},
		{function.COSH, types.T_float64},
		{function.ASINH, types.T_float64},
		{function.ACOSH, types.T_float64},
		{function.ATANH, types.T_float64},
		{function.FORMAT_BYTES, types.T_varchar},
		{function.FORMAT_PICO_TIME, types.T_varchar},
	} {
		p.InstructionList[0].ProjectList = append(p.InstructionList[0].ProjectList, &planpb.Expr{
			Typ: planpb.Type{Id: int32(tc.resultType)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, 0)},
				Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
			}},
		})
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.False(t, features.Any())
	require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, p, features))
	require.Zero(t, client.calls)
	require.Zero(t, client.releases)
	// An ordinary pipeline needs no capability evidence, even without a node.
	p.Node = nil
	require.NoError(t, validateRemoteExpressionDestination(nil, p, features))
	allocs := testing.AllocsPerRun(100, func() {
		if err := validateRemoteExpressionDestination(c.proc, p, features); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs, "ordinary sender capability gating must not allocate")
}

func TestViewMetadataSenderSharesExpressionProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	op := projection.NewArgument()
	t.Cleanup(op.Release)
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, tc := range []struct {
		id   int32
		name string
		typ  types.T
	}{
		{function.MO_VIEW_DEFINITION, "mo_view_definition", types.T_text},
		{function.MO_VIEW_CHECK_OPTION, "mo_view_check_option", types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.typ)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, 0), ObjName: tc.name},
				Args: []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("{}", false)},
			}}}
			for _, mode := range []string{"standalone", "mixed", "calendar", "statistical", "normalized_interval", "division_warning", "collation"} {
				t.Run(mode, func(t *testing.T) {
					op.ProjectList = []*planpb.Expr{expr}
					c.proc.SetStmtProfile(&process.StmtProfile{})
					c.proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
					c.proc.GetSessionInfo().SqlMode = ""
					c.proc.SetResolveVariableFunc(nil)
					if mode == "division_warning" {
						c.proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
						division, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
							{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
							plan2.MakePlan2Int64ConstExprWithType(0),
						})
						require.NoError(t, err)
						op.ProjectList = append(op.ProjectList, division)
					}
					if mode == "collation" {
						typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
						typ.CollationVersion = 1
						collation, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "least", []*planpb.Expr{
							{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
							{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
						})
						require.NoError(t, err)
						_, overload := function.DecodeOverloadID(collation.GetF().Func.Obj)
						require.Equal(t, int32(function.LeastGreatestCollationOverload), overload)
						op.ProjectList = append(op.ProjectList, collation)
					}
					if mode == "mixed" {
						op.ProjectList = append(op.ProjectList, decimalDivisionProtocolExpr(types.T_decimal128))
					}
					if mode == "calendar" {
						op.ProjectList = append(op.ProjectList, &planpb.Expr{
							Typ: planpb.Type{Id: int32(types.T_int32)},
							Expr: &planpb.Expr_F{F: &planpb.Function{
								Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.DAYOFYEAR, 0)},
								Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
							}},
						})
					}
					if mode == "statistical" {
						statistical, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "var_pop", []*planpb.Expr{{
							Typ:  types.New(types.T_decimal128, 38, 18).PlanType(),
							Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
						}})
						require.NoError(t, err)
						op.ProjectList = append(op.ProjectList, statistical)
					}
					if mode == "normalized_interval" {
						normalized, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "to_calendar_interval", []*planpb.Expr{
							{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
							plan2.MakePlan2Int64ConstExprWithType(int64(types.Day)),
						})
						require.NoError(t, err)
						op.ProjectList = append(op.ProjectList, normalized)
					}
					features, err := planpb.RequiredRemoteExpressionFeatures(&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: op.ProjectList}}})
					require.NoError(t, err)
					require.True(t, features.ViewMetadataFunctions, "shared analysis must match the actual registry IDs")
					require.Equal(t, mode == "mixed", features.DecimalDivisionSemantics)
					require.Equal(t, mode == "calendar", features.DateNumericResultContracts)
					require.Equal(t, mode == "statistical", features.DecimalStatisticalResultContracts)
					require.Equal(t, mode == "normalized_interval", features.CalendarIntervalNormalization)
					require.Equal(t, mode == "mixed" || mode == "division_warning", features.ScalarDivision)
					require.Equal(t, mode == "collation", features.LeastGreatestCollationSemantics)
					require.Equal(t, defines.MORPCVersion118, executionRemoteExpressionProtocolVersion(c.proc, features))
					require.Equal(t, defines.MORPCVersion118, remoteExpressionProtocolVersion(features))
					persistedVersion, err := plan2.RequiredPersistedExpressionProtocolVersion(op.ProjectList)
					require.NoError(t, err)
					require.Equal(t, defines.MORPCVersion118, persistedVersion)
					// No capability observation survives a send, including after
					// success followed by rollback of the very same endpoint.
					for _, version := range []int64{defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion118, defines.MORPCVersion117, defines.MORPCVersion116, defines.MORPCVersion115, defines.MORPCVersion114, defines.MORPCVersion113, defines.MORPCVersion112, 0} {
						client.version = version
						calls, releases := client.calls, client.releases
						data, err := encodeRemoteScope(scope, c.proc)
						if version == defines.MORPCVersion118 {
							require.NoError(t, err)
							require.NotEmpty(t, data)
						} else if version < defines.MORPCVersion117 && mode == "collation" {
							require.ErrorContains(t, err, "remote destination does not support collation-aware LEAST/GREATEST")
						} else if version < defines.MORPCVersion116 && mode == "division_warning" {
							require.ErrorContains(t, err, "remote destination does not support SELECT division diagnostics")
						} else if version == 0 && mode == "mixed" {
							require.ErrorContains(t, err, "remote destination does not support decimal division")
						} else if version < defines.MORPCVersion112 && mode == "calendar" {
							require.ErrorContains(t, err, "remote destination does not support signed date result contracts")
						} else if version < defines.MORPCVersion115 && mode == "normalized_interval" {
							require.ErrorContains(t, err, "remote destination does not support calendar interval normalization")
						} else if version < defines.MORPCVersion113 && mode == "statistical" {
							require.ErrorContains(t, err, "remote destination does not support decimal statistical DOUBLE results")
						} else {
							require.ErrorContains(t, err, "remote destination does not support view metadata functions (MORPC version 118)")
						}
						require.Equal(t, 1, client.calls-calls)
						require.Equal(t, 1, client.releases-releases)
					}
				})
			}
		})
	}
}

// Measure feature analysis plus the sender gate, excluding protobuf marshaling
// and fixture allocation. Bytes must affect serialization, but not this check.
func BenchmarkViewMetadataSenderProtocol(b *testing.B) {
	for _, kind := range []string{"membership", "literal_vec", "fold"} {
		for _, size := range []int{0, 1 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("%s_bytes=%d", kind, size), func(b *testing.B) {
				payload := make([]byte, size)
				p := &pipeline.Pipeline{}
				switch kind {
				case "membership":
					p.DataSource = &pipeline.Source{MembershipFilter: payload}
				case "literal_vec":
					p.InstructionList = []*pipeline.Instruction{{ProjectList: []*planpb.Expr{{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: payload}}}}}}
				case "fold":
					p.InstructionList = []*pipeline.Instruction{{ProjectList: []*planpb.Expr{{Expr: &planpb.Expr_Fold{Fold: &planpb.FoldVal{Data: payload}}}}}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					features, err := planpb.RequiredRemoteExpressionFeatures(p)
					if err != nil {
						b.Fatal(err)
					}
					if err := validateRemoteExpressionFeatures(nil, features); err != nil {
						b.Fatal(err)
					}
					if err := validateRemoteExpressionDestination(nil, p, features); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
