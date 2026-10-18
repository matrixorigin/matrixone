// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestConstantFoldBitCountDiagnostics(t *testing.T) {
	for _, folder := range []string{"generic", "rule", "prepared_rule"} {
		for _, value := range []string{"3 ", "3x"} {
			t.Run(folder+"/"+value, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				sink := &process.WarningProbe{}
				proc.WarningSink = sink
				expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "bit_count", []*Expr{makePlan2StringConstExprWithType(value)})
				require.NoError(t, err)
				var folded *Expr
				if folder == "generic" {
					folded, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
					require.NoError(t, err)
				} else {
					node := &planpb.Node{ProjectList: []*Expr{expr}}
					rule.NewConstantFold(folder == "prepared_rule").Apply(node, nil, proc)
					folded = node.ProjectList[0]
				}
				require.False(t, sink.Warned(), "speculation must not publish statement warnings")
				require.Same(t, sink, proc.WarningSink)
				if value == "3x" {
					require.NotNil(t, folded.GetF())
				} else {
					require.NotNil(t, folded.GetLit())
					require.Equal(t, uint64(2), folded.GetLit().GetU64Val())
				}
			})
		}
	}
}

// Wide explicit constants must remain valid in consumers that inspect their
// configuration or comparison expressions before execution.
func TestWideExplicitConstantConsumers(t *testing.T) {
	for _, sql := range []string{
		"select approx_percentile((? + cast(5 as decimal(65,1))) / 100.0) within group (order by n_nationkey desc) from nation",
		"select percentile_cont((? + cast(5 as decimal(65,1))) / 100.0) within group (order by n_nationkey) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.NotEmpty(t, preparedParamPositions(prepare))
			require.True(t, PreparedPlanHasPercentileParams(prepare.Plan))
		})
	}
	for _, predicate := range []string{
		"id=(select cast(3 as decimal(65,1)))",
		"id<(select cast(3 as decimal(65,1)))",
	} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, "select id from index_hint_t use index() where "+predicate)
			require.NoError(t, err)
			require.NotNil(t, query.GetQuery())
		})
	}
}

func TestDecimal256ConstantFoldBoundaries(t *testing.T) {
	for _, api := range []string{"public", "rule"} {
		t.Run(api, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			sink := &loadAssignmentWarningSink{}
			proc.WarningSink = sink
			wide := types.New(types.T_decimal256, 65, 1)
			makeCast := func(value string, null bool) *planpb.Expr {
				input := makePlan2StringConstExprWithType(value)
				input.GetLit().Isnull = null
				expr, err := appendExplicitCastBeforeExpr(proc.Ctx, input, wide.PlanType())
				require.NoError(t, err)
				return expr
			}
			fold := func(expr *planpb.Expr) *planpb.Expr {
				if api == "rule" {
					node := &planpb.Node{ProjectList: []*planpb.Expr{expr}}
					rule.NewConstantFold(false).Apply(node, nil, proc)
					return node.ProjectList[0]
				}
				got, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
				require.NoError(t, err)
				return got
			}
			for _, tc := range []struct {
				value    string
				null     bool
				overflow bool
			}{
				{value: "900.0"},
				{value: "1234567890123456789012345678901234567890123456789012345678901234.5"},
				{null: true},
				{value: "1" + strings.Repeat("0", 64), overflow: true},
			} {
				func() {
					sink.codes = nil
					expr := makeCast(tc.value, tc.null)
					originalType := expr.Typ
					got := fold(expr)
					require.Equal(t, originalType, got.Typ)
					require.Empty(t, sink.codes, "folding may not publish diagnostics")
					if tc.overflow {
						require.NotNil(t, got.GetF())
					} else if tc.null {
						require.True(t, got.GetLit().GetIsnull())
					} else {
						require.NotNil(t, got.GetVec())
						require.True(t, got.GetVec().DecimalLiteralRequiresV82)
						required, err := planpb.RequiresMORPCVersion89DecimalLiteralSemantics(got)
						require.NoError(t, err)
						require.True(t, required)
						encoded, err := got.Marshal()
						require.NoError(t, err)
						decoded := new(planpb.Expr)
						require.NoError(t, decoded.Unmarshal(encoded))
						got = decoded
					}
					input := batch.New(nil)
					defer input.Clean(proc.Mp())
					input.SetRowCount(3)
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, got, []*batch.Batch{input})
					require.NoError(t, err)
					defer free()
					if !tc.overflow {
						require.True(t, result.IsConst())
					}
					parameter := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](result)
					for row := range 3 {
						value, null := parameter.GetValue(uint64(row))
						require.Equal(t, tc.null, null)
						if !null {
							want := tc.value
							if tc.overflow {
								want = strings.Repeat("9", 64) + ".9"
							}
							require.Equal(t, want, value.Format(1))
						}
					}
					if tc.overflow {
						require.Equal(t, []uint16{1264, 1264, 1264}, sink.codes)
					}
				}()
			}
			abs, err := function.GetFunctionByName(proc.Ctx, "abs", []types.Type{wide})
			require.NoError(t, err)
			nested := fold(&planpb.Expr{Typ: wide.PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "abs", Obj: abs.GetEncodedOverloadID()}, Args: []*planpb.Expr{makeCast("-900.0", false)},
			}}})
			require.NotNil(t, nested.GetVec(), "parents must still fold above the new scalar representation")
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, nested, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, "900.0", vector.GetFixedAtNoTypeCheck[types.Decimal256](result, 0).Format(1))
			for _, target := range []types.Type{wide, types.New(types.T_decimal64, 10, 1)} {
				nestedCast, err := appendExplicitCastBeforeExpr(proc.Ctx, makeCast("900.0", false), target.PlanType())
				require.NoError(t, err)
				folded := fold(nestedCast)
				if target.Oid == types.T_decimal64 {
					require.NotNil(t, folded.GetLit(), "a scalar vector child must not turn a narrow scalar into a list")
				}
				value, cleanup, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer cleanup()
				require.True(t, value.IsConst(), "a silent nested CAST must retain scalar broadcasting")
			}
		})
	}
}

func TestConstantFoldPreservesSerializedLiteralProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name           string
		function       string
		inputType      types.Type
		input          *planpb.Literal
		wantNull       bool
		wantSerialized bool
	}{
		{
			name:           "serial",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial null",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantNull:       true,
			wantSerialized: false,
		},
		{
			name:           "serial full",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial full null",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantSerialized: true,
		},
		{
			name:           "ordinary string function control",
			function:       "lower",
			inputType:      types.T_varchar.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "VISIBLE"}},
			wantSerialized: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered, err := function.GetFunctionByName(context.Background(), test.function, []types.Type{test.inputType})
			require.NoError(t, err)

			expr := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: test.function},
					Args: []*planpb.Expr{{
						Typ:  planpb.Type{Id: int32(test.inputType.Oid)},
						Expr: &planpb.Expr_Lit{Lit: test.input},
					}},
				}},
			}

			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
			require.NoError(t, err)
			literal := folded.GetLit()
			require.NotNil(t, literal)
			require.Equal(t, test.wantNull, literal.GetIsnull())
			require.False(t, literal.GetIsBin(), "folding must not acquire SQL hex/bit semantics")
			require.Equal(t, test.wantSerialized, literal.GetIsSerialized())

			copied := DeepCopyExpr(folded)
			require.Equal(t, test.wantSerialized, copied.GetLit().GetIsSerialized())

			payload, err := proto.Marshal(folded)
			require.NoError(t, err)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.Equal(t, test.wantSerialized, decoded.GetLit().GetIsSerialized())
		})
	}
}

func TestOptimizerConstantFoldsNegativeSecToTime(t *testing.T) {
	stmt, err := mysql.ParseOne(t.Context(), "select sec_to_time(-2378)", 1)
	require.NoError(t, err)

	query, err := NewBaseOptimizer(NewMockCompilerContext(true, newPlanTestProcess(t))).Optimize(stmt, false)
	require.NoError(t, err)
	require.NotEmpty(t, query.Steps)
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotEmpty(t, root.ProjectList)

	literal := root.ProjectList[0].GetLit()
	require.NotNil(t, literal)
	require.Equal(t, int64(-2378*types.MicroSecsPerSec), literal.GetTimeval())
}

func TestConstantFoldDefersLegacyTimeAssignmentCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.T_varchar.ToType()
	timeType := types.T_time.ToTypeWithScale(6)
	registered, err := function.GetFunctionByName(
		context.Background(), "cast_strict", []types.Type{inputType, timeType},
	)
	require.NoError(t, err)

	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_time), Scale: 6},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: "cast_strict"},
			Args: []*planpb.Expr{
				{
					Typ:  planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "2562047788:00:00"}}},
				},
				{
					Typ:  planpb.Type{Id: int32(types.T_time), Scale: 6},
					Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
				},
			},
		}},
	}

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetF())
	require.Equal(t, "cast_strict", folded.GetF().GetFunc().GetObjName())
}

func TestConstantFoldPreservesSerialCastSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	registered, err := function.GetFunctionByName(
		context.Background(), function.SerialFunctionName, []types.Type{types.T_bool.ToType()},
	)
	require.NoError(t, err)

	serialExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     registered.GetEncodedOverloadID(),
				ObjName: function.SerialFunctionName,
			},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}},
			}},
		}},
	}
	castExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "cast", []*planpb.Expr{
		serialExpr,
		{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
		},
	})
	require.NoError(t, err)

	runtimeResult, runtimeFree, runtimeErr := colexec.GetReadonlyResultFromExpression(
		proc, DeepCopyExpr(castExpr), []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	if runtimeFree != nil {
		defer runtimeFree()
	}

	folded, foldErr := ConstantFold(
		batch.EmptyForConstFoldBatch, DeepCopyExpr(castExpr), proc, false, true,
	)
	require.Equal(t, runtimeErr != nil, foldErr != nil, "constant folding changed whether the expression fails")
	if runtimeErr != nil {
		return
	}

	foldedResult, foldedFree, err := colexec.GetReadonlyResultFromExpression(
		proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, err)
	defer foldedFree()
	require.Equal(
		t,
		rule.GetConstantValue(runtimeResult, false, 0),
		rule.GetConstantValue(foldedResult, false, 0),
		"constant folding changed the expression value",
	)
}

func TestReplaceFoldExprKeepsChildWhenConstantFoldFails(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.New(types.T_decimal256, 65, 30)
	badCast, err := makePlan2CastExpr(
		context.Background(),
		MakePlan2StringConstExprWithType("not-a-decimal"),
		makePlan2Type(&target),
	)
	require.NoError(t, err)

	column := &planpb.Expr{
		Typ: makePlan2Type(&target),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			Name: "d",
		}},
	}
	filter, err := BindFuncExprImplByPlanExpr(
		context.Background(), "=", []*planpb.Expr{column, badCast},
	)
	require.NoError(t, err)

	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	_, err = ReplaceFoldExpr(proc, filter, &executors)
	require.Error(t, err)
	require.NotNil(t, filter.GetF().Args[1], "a failed fold must not replace its child with nil")
	require.Equal(t, "cast", filter.GetF().Args[1].GetF().GetFunc().GetObjName())
}

func TestOptimizerPreservesByteIdenticalSerializedProvenance(t *testing.T) {
	for _, test := range []struct {
		name           string
		sql            string
		wantValue      string
		wantSerialized bool
	}{
		{
			name:           "lossless cast",
			sql:            "select cast(serial(true) as blob)",
			wantValue:      string([]byte{0x27}),
			wantSerialized: true,
		},
		{
			name:           "transformed value control",
			sql:            "select hex(serial(true))",
			wantValue:      "27",
			wantSerialized: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)

			query, err := NewBaseOptimizer(NewMockCompilerContext(true, newPlanTestProcess(t))).Optimize(stmt, false)
			require.NoError(t, err)
			require.NotEmpty(t, query.Steps)
			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.NotEmpty(t, root.ProjectList)

			literal := root.ProjectList[0].GetLit()
			require.NotNil(t, literal)
			require.Equal(t, test.wantValue, literal.GetSval())
			require.Equal(t, test.wantSerialized, literal.GetIsSerialized())
		})
	}
}

func TestOptimizerPreservesSerializedListProvenance(t *testing.T) {
	for _, test := range []struct {
		name           string
		sql            string
		wantSerialized bool
	}{
		{
			name: "serialized decimal list",
			sql: "select n_name from nation where n_name in (" +
				"serial(cast(99999 as decimal(38,0))), " +
				"serial(cast(100000 as decimal(38,0))))",
			wantSerialized: true,
		},
		{
			name:           "ordinary unicode list control",
			sql:            "select n_name from nation where n_name in ('Résumé', '東京')",
			wantSerialized: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			query, err := NewBaseOptimizer(NewMockCompilerContext(true, newPlanTestProcess(t))).Optimize(stmt, false)
			require.NoError(t, err)

			literalVecExpr := findFirstLiteralVecExpr(query)
			require.NotNil(t, literalVecExpr)
			require.Equal(t, test.wantSerialized, literalVecExpr.GetVec().GetIsSerialized())

			copied := DeepCopyExpr(literalVecExpr)
			require.Equal(t, test.wantSerialized, copied.GetVec().GetIsSerialized())

			payload, err := proto.Marshal(literalVecExpr)
			require.NoError(t, err)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.Equal(t, test.wantSerialized, decoded.GetVec().GetIsSerialized())
		})
	}
}

func TestOptimizerDoesNotTreatSerializedProvenanceAsFilterValue(t *testing.T) {
	for _, test := range []struct {
		name       string
		serialized string
		wantFalse  bool
	}{
		{name: "byte-identical values", serialized: "true", wantFalse: false},
		{name: "different values control", serialized: "false", wantFalse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(
				t.Context(),
				"select n_name from nation where n_name = serial("+test.serialized+") and n_name = ''''",
				1,
			)
			require.NoError(t, err)

			query, err := NewBaseOptimizer(NewMockCompilerContext(true, newPlanTestProcess(t))).Optimize(stmt, false)
			require.NoError(t, err)

			seenScan := false
			for _, node := range query.Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN {
					continue
				}
				seenScan = true
				require.NotEmpty(t, node.FilterList)
				hasFalse := false
				for _, filter := range node.FilterList {
					hasFalse = hasFalse || IsFalseExpr(filter)
				}
				require.Equal(t, test.wantFalse, hasFalse,
					"filter-domain normalization changed serialized byte-value semantics")
			}
			require.True(t, seenScan)
		})
	}
}

func TestConstantFoldPreservesSerializedListProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringLiteral := func(value string, serialized bool) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:        &planpb.Literal_Sval{Sval: value},
				IsSerialized: serialized,
			}},
		}
	}
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			stringLiteral(string([]byte{0x27}), true),
			stringLiteral("ordinary", false),
		}}},
	}

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetVec())
	require.True(t, folded.GetVec().GetIsSerialized())
}

func TestConstantListFoldPreservesPerItemStringProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryType := planpb.Type{
		Id:      int32(types.T_varbinary),
		Charset: uint32(types.CharsetBinary),
	}
	literal := func(form planpb.StringLiteralForm) *planpb.Expr {
		return &planpb.Expr{
			Typ: binaryType,
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value:       &planpb.Literal_Sval{Sval: "same-bytes"},
				LiteralForm: form,
			}},
		}
	}
	crossDomainList := &planpb.Expr{
		Typ: binaryType,
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			literal(planpb.StringLiteralForm_STRING_LITERAL_TEXT),
			literal(planpb.StringLiteralForm_STRING_LITERAL_NONE),
		}}},
	}

	required, err := planpb.RequiresMORPCVersion23StringProvenance(crossDomainList)
	require.NoError(t, err)
	require.True(t, required)

	foldWithRule := func() *planpb.Expr {
		node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(crossDomainList)}}
		rule.NewConstantFold(false).Apply(node, nil, proc)
		return node.ProjectList[0]
	}
	foldWithPublicAPI := func() *planpb.Expr {
		folded, foldErr := ConstantFold(
			batch.EmptyForConstFoldBatch, DeepCopyExpr(crossDomainList), proc, false, true,
		)
		require.NoError(t, foldErr)
		return folded
	}

	for name, folded := range map[string]*planpb.Expr{
		"rule":   foldWithRule(),
		"public": foldWithPublicAPI(),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, folded.GetList(),
				"LiteralVec cannot encode per-item runtime string domains")
			requiredAfterFold, requireErr := planpb.RequiresMORPCVersion23StringProvenance(folded)
			require.NoError(t, requireErr)
			require.True(t, requiredAfterFold)

			result, free, evalErr := colexec.GetReadonlyResultFromExpression(
				proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch},
			)
			require.NoError(t, evalErr)
			defer free()
			require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(0))
			require.Equal(t, types.RuntimeStringInherit, result.GetRuntimeStringDomainAt(1))
		})
	}

	textType := planpb.Type{Id: int32(types.T_varchar)}
	ordinaryLiteral := &planpb.Expr{
		Typ: textType,
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value:       &planpb.Literal_Sval{Sval: "ordinary"},
			LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_TEXT,
		}},
	}
	ordinaryList := &planpb.Expr{
		Typ: textType,
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			ordinaryLiteral, DeepCopyExpr(ordinaryLiteral),
		}}},
	}
	foldedControl, err := ConstantFold(
		batch.EmptyForConstFoldBatch, ordinaryList, proc, false, true,
	)
	require.NoError(t, err)
	require.NotNil(t, foldedControl.GetVec(), "same-domain list keeps the existing fold fast path")
	require.Equal(t, int32(1), foldedControl.GetVec().GetLen(), "duplicate literals compact to one row")
	require.Equal(t, uint32(types.StringSourceLiteral), foldedControl.GetVec().GetStringSource())
	require.Equal(t, foldedControl.GetVec().GetStringSource(),
		DeepCopyExpr(foldedControl).GetVec().GetStringSource())
	payload, err := proto.Marshal(foldedControl)
	require.NoError(t, err)
	decodedControl := new(planpb.Expr)
	require.NoError(t, proto.Unmarshal(payload, decodedControl))
	require.Equal(t, foldedControl.GetVec().GetStringSource(),
		decodedControl.GetVec().GetStringSource())
	result, free, evalErr := colexec.GetReadonlyResultFromExpression(
		proc, foldedControl, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, evalErr)
	defer free()
	require.Equal(t, types.StringSourceLiteral, result.GetStringSourceAt(0))
}

func TestConstantFoldPreservesExplicitCastSource(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), "select cast('x' as char)", 1)
	require.NoError(t, err)
	pl, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	query := pl.GetQuery()
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.Len(t, root.ProjectList, 1)
	unfolded := root.ProjectList[0]
	require.NotNil(t, unfolded.GetF())

	node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(unfolded)}}
	rule.NewConstantFold(false).Apply(node, nil, ctx.GetProcess())
	folded := node.ProjectList[0]
	require.NotNil(t, folded.GetLit())
	require.Equal(t, uint32(types.StringSourceExpression)+1, folded.GetLit().GetStringSource())

	copied := DeepCopyExpr(folded)
	require.Equal(t, folded.GetLit().GetStringSource(), copied.GetLit().GetStringSource())
	require.Equal(t, exprStructuralHash(folded), exprStructuralHash(copied))
	require.True(t, exprStructuralEqual(folded, copied))
	payload, err := proto.Marshal(folded)
	require.NoError(t, err)
	decoded := new(planpb.Expr)
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, folded.GetLit().GetStringSource(), decoded.GetLit().GetStringSource())
	require.Equal(t, exprStructuralHash(folded), exprStructuralHash(decoded))
	require.True(t, exprStructuralEqual(folded, decoded))

	result, free, err := colexec.GetReadonlyResultFromExpression(
		ctx.GetProcess(), decoded, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, types.StringSourceExpression, result.GetStringSourceAt(0))

	plain := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: "x"},
		}},
	}
	plainResult, plainFree, err := colexec.GetReadonlyResultFromExpression(
		ctx.GetProcess(), plain, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer plainFree()
	require.Equal(t, types.StringSourceLiteral, plainResult.GetStringSourceAt(0))
}

func TestMakeInExprRuntimePayloadKeepsExpressionSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	scanKeys := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(scanKeys, []byte("runtime-key"), false, proc.Mp()))
	require.Equal(t, types.StringSourceExpression, scanKeys.GetStringSourceAt(0))
	data, err := scanKeys.MarshalBinary()
	require.NoError(t, err)
	scanKeys.Free(proc.Mp())

	typ := planpb.Type{Id: int32(types.T_varchar)}
	inExpr := MakeInExpr(t.Context(), &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}, 1, data, false)
	runtimePayload := inExpr.GetF().Args[1]
	require.Zero(t, runtimePayload.GetVec().GetStringSource())

	result, free, evalErr := colexec.GetReadonlyResultFromExpression(
		proc, runtimePayload, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, evalErr)
	defer free()
	require.Equal(t, types.StringSourceExpression, result.GetStringSourceAt(0))
}

func TestConstantFoldPreservesSelectedStringDomain(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantDomain types.RuntimeStringDomain
		wantFold   bool
	}{
		{name: "if text to binary", sql: "select if(true, 'selected', cast('fallback' as varbinary))", wantDomain: types.RuntimeStringText, wantFold: true},
		{name: "case runtime control", sql: "select case when true then 'selected' else cast('fallback' as varbinary) end", wantDomain: types.RuntimeStringText},
		{name: "coalesce binary to text", sql: "select coalesce(_binary'selected', cast('fallback' as char))", wantDomain: types.RuntimeStringBinary, wantFold: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			pl, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			require.NotEmpty(t, query.Steps)
			root := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.Len(t, root.ProjectList, 1)
			expr := root.ProjectList[0]

			foldWithRule := func() *planpb.Expr {
				node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(expr)}}
				rule.NewConstantFold(false).Apply(node, nil, ctx.GetProcess())
				return node.ProjectList[0]
			}
			foldWithPublicAPI := func() *planpb.Expr {
				folded, foldErr := ConstantFold(
					batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), ctx.GetProcess(), false, true)
				require.NoError(t, foldErr)
				return folded
			}

			for name, folded := range map[string]*planpb.Expr{
				"rule":   foldWithRule(),
				"public": foldWithPublicAPI(),
			} {
				t.Run(name, func(t *testing.T) {
					if test.wantFold {
						require.NotNil(t, folded.GetLit())
					} else {
						require.NotNil(t, folded.GetF())
					}
					result, free, evalErr := colexec.GetReadonlyResultFromExpression(
						ctx.GetProcess(), folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, evalErr)
					defer free()
					require.Equal(t, "selected", result.GetStringAt(0))
					require.Equal(t, test.wantDomain, result.GetRuntimeStringDomainAt(0))
				})
			}
		})
	}
}

func TestConstantFoldDynamicIPFunctionLosesFunctionNode(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), "select inet_ntoa('1.6')", 1)
	require.NoError(t, err)
	defer stmt.Free()

	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewGeneratedColBinder(ctx.GetProcess().Ctx, nil, nil)
	bound, err := binder.BindExpr(ast, 0, false)
	require.NoError(t, err)
	require.NotNil(t, bound.GetF(), "the pre-optimization plan must retain the dynamic overload")
	requiredBefore, err := RequiredPersistedExpressionProtocolVersion(bound)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion86), requiredBefore)

	folded, err := ConstantFold(
		batch.EmptyForConstFoldBatch, DeepCopyExpr(bound), ctx.GetProcess(), false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetLit(), "the optimizer can fold the constant dynamic overload")
	requiredAfter, err := RequiredPersistedExpressionProtocolVersion(folded)
	require.NoError(t, err)
	require.Zero(t, requiredAfter,
		"the folded literal no longer exposes the function node; view DDL must retain the pre-fold requirement")
}

func findFirstLiteralVecExpr(query *planpb.Query) *planpb.Expr {
	var found *planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil || found != nil {
			return
		}
		if expr.GetVec() != nil {
			found = expr
			return
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				visit(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	for _, node := range query.Nodes {
		for _, filter := range node.FilterList {
			visit(filter)
		}
	}
	return found
}

func TestConstantFoldDefersGuardedKernelFailure(t *testing.T) {
	for _, selector := range []struct {
		name     string
		constant bool
	}{{"case", false}, {"case", true}, {"if", false}, {"coalesce", false}} {
		name := selector.name
		for _, public := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/constant=%t/public=%t", name, selector.constant, public), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				t.Cleanup(func() {
					defer proc.Free()
					require.Equal(t, [2]int64{}, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()})
				})
				bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
					expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
					require.NoError(t, err)
					return expr
				}
				bad := bind("round", MakePlan2Int64ConstExprWithType(5000000000000000000), MakePlan2Int64ConstExprWithType(-19))
				safe := bind("abs", MakePlan2Int64ConstExprWithType(-7))
				input := batch.NewWithSize(1)
				input.SetRowCount(1)
				conditionType := types.T_bool
				input.Vecs[0] = testutil.MakeBoolVector([]bool{false}, nil, proc.Mp())
				if name == "coalesce" {
					input.Vecs[0].Free(proc.Mp())
					conditionType = types.T_int64
					input.Vecs[0] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
				}
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				condition := &planpb.Expr{Typ: planpb.Type{Id: int32(conditionType)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
				if selector.constant {
					condition = makePlan2BoolConstExprWithType(false)
				}
				expr := bind(name, condition, bad, safe)
				var folded *planpb.Expr
				if public {
					var err error
					folded, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
					require.NoError(t, err)
				} else {
					node := &planpb.Node{ProjectList: []*planpb.Expr{expr}}
					rule.NewConstantFold(false).Apply(node, nil, proc)
					folded = node.ProjectList[0]
				}
				require.NotNil(t, folded.GetF())
				require.NotNil(t, folded.GetF().Args[1].GetF())
				fid, _ := function.DecodeOverloadID(folded.GetF().Args[1].GetF().Func.Obj)
				require.Equal(t, int32(function.ROUND), fid, "retain the failing subtree")
				require.Equal(t, int64(7), folded.GetF().Args[2].GetLit().GetI64Val(), "safe sibling still folds")
				baseline := [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()}
				func() {
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{input})
					require.NoError(t, err)
					defer free()
					require.Equal(t, int64(7), vector.MustFixedColNoTypeCheck[int64](result)[0])
				}()
				require.Equal(t, baseline, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()})
				if selector.constant {
					folded.GetF().Args[0].GetLit().Value.(*planpb.Literal_Bval).Bval = true
				} else if name == "coalesce" {
					input.Vecs[0].GetNulls().Add(0)
				} else {
					vector.MustFixedColNoTypeCheck[bool](input.Vecs[0])[0] = true
				}
				baseline = [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()}
				var escaped any
				func() {
					defer func() { escaped = recover() }()
					_, free, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{input})
					if free != nil {
						defer free()
					}
					require.NoError(t, err)
				}()
				panicErr, ok := escaped.(error)
				require.True(t, ok, "selected ROUND must still fail")
				require.True(t, moerr.IsMoErrCode(panicErr, moerr.ErrOutOfRange))
				require.Equal(t, baseline, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()})
			})
		}
	}
}
